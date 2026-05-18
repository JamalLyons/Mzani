//! In-process proxy and mock backend harness for integration tests.

use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mzani::{Context, MzaniError, MzaniResult, ThreadPool};

use super::http::{HttpRequestSpec, HttpResponse, RecordedRequest, exchange, read_request, write_raw_response};

/// Response a mock backend returns to the proxy.
#[derive(Debug, Clone)]
pub enum BackendReply
{
    Raw(&'static [u8]),
    OkText(&'static str),
    NotFound(&'static str),
    NoContent,
    Chunked(&'static str),
    Delayed
    {
        wait: Duration,
        inner: &'static [u8],
    },
}

impl BackendReply
{
    fn write_to(&self, stream: &mut TcpStream) -> MzaniResult<()>
    {
        match self {
            Self::Raw(bytes) => write_raw_response(stream, bytes),
            Self::OkText(body) => write_raw_response(
                stream,
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes(),
            ),
            Self::NotFound(body) => write_raw_response(
                stream,
                format!("HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes(),
            ),
            Self::NoContent => write_raw_response(stream, b"HTTP/1.1 204 No Content\r\n\r\n"),
            Self::Chunked(body) => {
                let mut raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
                raw.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
                raw.extend_from_slice(body.as_bytes());
                raw.extend_from_slice(b"\r\n0\r\n\r\n");
                write_raw_response(stream, &raw)
            }
            Self::Delayed { wait, inner } => {
                thread::sleep(*wait);
                write_raw_response(stream, inner)
            }
        }
    }
}

/// Mock backend that records proxied requests and returns a fixed reply.
pub struct MockBackend
{
    pub addr: SocketAddr,
    #[allow(dead_code)]
    pub label: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    handle: JoinHandle<MzaniResult<()>>,
}

impl MockBackend
{
    /// Starts a backend that accepts `accepts` connections then stops.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] if binding fails.
    pub fn spawn(label: impl Into<String>, reply: BackendReply, accepts: usize) -> MzaniResult<Self>
    {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let label = label.into();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::clone(&recorded);

        let handle = thread::spawn(move || -> MzaniResult<()> {
            let mut served = 0usize;
            while served < accepts {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request = read_request(&mut stream)?;
                        store
                            .lock()
                            .map_err(|_| MzaniError::ParseError("record lock poisoned".to_owned()))?
                            .push(request);
                        reply.write_to(&mut stream)?;
                        served += 1;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        });

        Ok(Self {
            addr,
            label,
            recorded,
            handle,
        })
    }

    /// Returns recorded requests in arrival order.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] if the mutex is poisoned.
    pub fn recordings(&self) -> MzaniResult<Vec<RecordedRequest>>
    {
        self.recorded
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| MzaniError::ParseError("record lock poisoned".to_owned()))
    }

    /// Waits until the backend thread exits.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] if the thread panicked.
    pub fn join(self) -> MzaniResult<()>
    {
        self.handle
            .join()
            .map_err(|_| MzaniError::ParseError("mock backend thread panicked".to_owned()))?
    }
}

/// Running load balancer instance for integration tests.
pub struct ProxyHarness
{
    pub proxy_addr: SocketAddr,
    listener: TcpListener,
    pool: ThreadPool,
    _context: Arc<RwLock<Context>>,
}

impl ProxyHarness
{
    /// Binds an ephemeral listen address and starts the worker pool.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] if binding, pool startup, or logger init fails.
    pub fn start(backends: Vec<SocketAddr>, log_dir: &Path) -> MzaniResult<Self>
    {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let proxy_addr = listener.local_addr()?;

        let context = Arc::new(RwLock::new(Context::new_with_options(
            backends,
            proxy_addr,
            Some(log_dir.to_path_buf()),
        )?));
        let pool = ThreadPool::new(&context)?;

        Ok(Self {
            proxy_addr,
            listener,
            pool,
            _context: context,
        })
    }

    /// Accepts one client connection and hands it to the worker pool.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::PoolFull`] or I/O errors from accept.
    pub fn accept_one(&mut self) -> MzaniResult<()>
    {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                    return self.pool.submit(stream).map_err(|_| MzaniError::PoolFull);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

/// Sends a request through the proxy while accepting the inbound connection on another stack frame.
///
/// # Errors
///
/// Returns [`MzaniError`] on accept, pool, or client I/O failures.
pub fn exchange_via_proxy(proxy: &mut ProxyHarness, spec: &HttpRequestSpec) -> MzaniResult<HttpResponse>
{
    let proxy_addr = proxy.proxy_addr;
    thread::scope(|scope| -> MzaniResult<HttpResponse> {
        let accept = scope.spawn(|| proxy.accept_one());
        let response = exchange(proxy_addr, spec)?;
        accept
            .join()
            .map_err(|_| MzaniError::ParseError("proxy accept panicked".to_owned()))??;
        Ok(response)
    })
}
