//! In-process proxy and mock backend harness for integration tests.

use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Serializes proxy exchanges across parallel integration tests (avoids accept/client races).
static PROXY_EXCHANGE_LOCK: Mutex<()> = Mutex::new(());

fn lock_proxy_exchange() -> MzaniResult<MutexGuard<'static, ()>> {
    PROXY_EXCHANGE_LOCK
        .lock()
        .map_err(|_| MzaniError::ParseError("proxy exchange lock poisoned".to_owned()))
}

use mzani::{Context, MzaniError, MzaniResult, PoolConfig, ThreadPool};

use super::http::{HttpRequestSpec, HttpResponse, RecordedRequest, exchange, read_request, write_raw_response};

/// Response a mock backend returns to the proxy.
#[derive(Debug, Clone)]
pub enum BackendReply {
    Raw(&'static [u8]),
    OkText(&'static str),
    NotFound(&'static str),
    NoContent,
    Chunked(&'static str),
    Delayed { wait: Duration, inner: &'static [u8] },
}

impl BackendReply {
    fn write_to(&self, stream: &mut TcpStream) -> MzaniResult<()> {
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
pub struct MockBackend {
    pub addr: SocketAddr,
    #[allow(dead_code)]
    pub label: String,
    expected_accepts: usize,
    handled: Arc<AtomicUsize>,
    recorded: Arc<std::sync::Mutex<Vec<RecordedRequest>>>,
    handle: JoinHandle<MzaniResult<()>>,
}

impl MockBackend {
    /// Starts a backend that accepts `accepts` connections then stops.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] if binding fails.
    pub fn spawn(label: impl Into<String>, reply: BackendReply, accepts: usize) -> MzaniResult<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let addr = listener.local_addr()?;
        let label = label.into();
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let store = Arc::clone(&recorded);
        let handled = Arc::new(AtomicUsize::new(0));
        let handled_for_thread = Arc::clone(&handled);

        let handle = thread::spawn(move || -> MzaniResult<()> {
            for _ in 0..accepts {
                let (mut stream, _) = listener.accept()?;
                let request = read_request(&mut stream)?;
                store
                    .lock()
                    .map_err(|_| MzaniError::ParseError("record lock poisoned".to_owned()))?
                    .push(request);
                reply.write_to(&mut stream)?;
                handled_for_thread.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        });

        Ok(Self {
            addr,
            label,
            expected_accepts: accepts,
            handled,
            recorded,
            handle,
        })
    }

    /// Returns recorded requests in arrival order.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] if the mutex is poisoned.
    pub fn recordings(&self) -> MzaniResult<Vec<RecordedRequest>> {
        self.recorded
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| MzaniError::ParseError("record lock poisoned".to_owned()))
    }

    /// Waits until the backend thread exits.
    ///
    /// The mock accepts exactly `accepts` connections (see [`Self::spawn`]). If the test
    /// sends fewer proxied requests, this call blocks until the thread is interrupted.
    /// A timeout returns a descriptive error instead of hanging the test runner.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] if the thread panicked or join times out.
    pub fn join(self) -> MzaniResult<()> {
        const JOIN_TIMEOUT: Duration = Duration::from_secs(30);
        let deadline = Instant::now() + JOIN_TIMEOUT;
        while Instant::now() < deadline {
            if self.handle.is_finished() {
                return self
                    .handle
                    .join()
                    .map_err(|_| MzaniError::ParseError("mock backend thread panicked".to_owned()))?;
            }
            thread::sleep(Duration::from_millis(10));
        }

        let handled = self.handled.load(Ordering::Relaxed);
        Err(MzaniError::ParseError(format!(
            "mock backend {:?} join timed out after {}s: handled {handled}/{} connections \
             (spawn count must match the number of proxied requests)",
            self.label,
            JOIN_TIMEOUT.as_secs(),
            self.expected_accepts
        )))
    }
}

/// Running load balancer instance for integration tests.
pub struct ProxyHarness {
    pub proxy_addr: SocketAddr,
    listener: TcpListener,
    pool: ThreadPool,
    shutting_down: Arc<AtomicBool>,
}

impl ProxyHarness {
    /// Binds an ephemeral listen address and starts the worker pool.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] if binding, pool startup, or logger init fails.
    pub fn start(backends: Vec<SocketAddr>, log_dir: &Path) -> MzaniResult<Self> {
        Self::start_with_pool(
            backends,
            log_dir,
            PoolConfig {
                workers: 4,
                per_worker_queue: 8,
                ..PoolConfig::default()
            },
        )
    }

    /// Starts a proxy with an explicit [`PoolConfig`] (for saturation tests).
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] if binding or pool startup fails.
    pub fn start_with_pool(backends: Vec<SocketAddr>, log_dir: &Path, pool_config: PoolConfig) -> MzaniResult<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        // Blocking accept pairs reliably with the test client in `exchange_via_proxy`.
        listener.set_nonblocking(false)?;
        let proxy_addr = listener.local_addr()?;

        let context = Context::new_with_options(backends, proxy_addr, Some(log_dir.to_path_buf()))?;
        let shutting_down = Arc::new(AtomicBool::new(false));
        let pool = ThreadPool::new(context, &pool_config, Arc::clone(&shutting_down))?;

        Ok(Self {
            proxy_addr,
            listener,
            pool,
            shutting_down,
        })
    }

    /// Accepts one client connection and hands it to the worker pool.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::PoolFull`] or I/O errors from accept.
    pub fn accept_one(&mut self) -> MzaniResult<()> {
        let (stream, _) = self.listener.accept()?;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
        match self.pool.submit(stream) {
            Ok(()) => Ok(()),
            Err((MzaniError::PoolFull, stream)) => {
                self.pool.reject_pool_full(stream);
                Ok(())
            }
            Err((error, _)) => Err(error),
        }
    }

    /// Worker pool reference for metrics and health assertions.
    #[must_use]
    pub fn pool(&self) -> &ThreadPool {
        &self.pool
    }
}

impl Drop for ProxyHarness {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        let _ = self.pool.shutdown_threads();
    }
}

/// Sends a request through the proxy while accepting the inbound connection on another stack frame.
///
/// # Errors
///
/// Returns [`MzaniError`] on accept, pool, or client I/O failures.
pub fn exchange_via_proxy(proxy: &mut ProxyHarness, spec: &HttpRequestSpec) -> MzaniResult<HttpResponse> {
    let _guard = lock_proxy_exchange()?;
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
