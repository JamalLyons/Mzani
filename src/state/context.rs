use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};

use crate::core::request::Request;
use crate::state::metrics::{Metrics, RequestStats};
use crate::utils::Bytes;
use crate::utils::logger::Logger;
use crate::{MzaniError, MzaniResult};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:5000";
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared runtime state for the load balancer.
///
/// Holds listen configuration, round-robin backend selection, metrics, and logging.
#[derive(Debug)]
pub struct Context
{
    idx: usize,
    socket_addr: SocketAddr,
    target_servers: Vec<SocketAddr>,
    metrics: Metrics,
    logger: Logger,
}

impl Context
{
    /// Creates a load balancer context with the given backend servers.
    ///
    /// # Arguments
    ///
    /// * `server_list` - Non-empty list of backend socket addresses
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::EmptyServerList`] if `server_list` is empty.
    /// Returns [`MzaniError::LoggerInit`] if logging cannot be initialized.
    #[must_use = "context must be constructed to run the server"]
    pub fn new(server_list: Vec<SocketAddr>) -> MzaniResult<Self>
    {
        if server_list.is_empty() {
            return Err(MzaniError::EmptyServerList);
        }

        Ok(Self {
            idx: 0,
            socket_addr: Self::listen_addr_from_env()?,
            target_servers: server_list,
            metrics: Metrics::default(),
            logger: Logger::new()?,
        })
    }

    /// Returns the socket address the load balancer listens on.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr
    {
        self.socket_addr
    }

    /// Returns a formatted metrics report.
    #[must_use]
    pub fn metrics_report(&self) -> String
    {
        self.metrics.format_report()
    }

    fn listen_addr_from_env() -> MzaniResult<SocketAddr>
    {
        let addr_str = env::args().nth(1).unwrap_or_else(|| DEFAULT_LISTEN_ADDR.to_owned());

        match addr_str.parse::<SocketAddr>() {
            Ok(addr) => Ok(addr),
            Err(_) => DEFAULT_LISTEN_ADDR
                .parse::<SocketAddr>()
                .map_err(|error| MzaniError::ParseError(error.to_string())),
        }
    }

    fn fetch_target_server(&mut self) -> SocketAddr
    {
        let target_addr = self.target_servers[self.idx];
        self.idx = (self.idx + 1) % self.target_servers.len();
        target_addr
    }

    /// Proxies a single client TCP connection to a backend server.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] on I/O, parse, or backend failures.
    pub fn handle_connection(&mut self, mut stream: TcpStream) -> MzaniResult<()>
    {
        stream.set_read_timeout(Some(READ_TIMEOUT))?;
        let start_time = Instant::now();
        let target = self.fetch_target_server();

        let request = Request::from_stream(&mut stream)?;
        let request_bytes = request.to_bytes();
        let request_len = request_bytes.len();

        let host = request
            .headers()
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("host"))
            .map_or("-", |(_, value)| value.as_str());

        let log_line = format!(
            "Routing {} {} (host: {host}, body: {} bytes) to {target}",
            request.method().as_str(),
            request.path(),
            request.body().len(),
        );
        self.logger.log(&log_line)?;

        if let Ok(preview) = std::str::from_utf8(&request_bytes[..request_bytes.len().min(512)]) {
            self.logger.log(preview)?;
        }

        let backend_response = Self::proxy_to_backend(&request_bytes, target)?;
        let response_len = backend_response.len();

        stream.write_all(&backend_response)?;
        stream.flush()?;
        stream.shutdown(std::net::Shutdown::Write)?;

        let duration = start_time.elapsed();
        self.metrics.record_request(RequestStats {
            request_len,
            response_len,
            duration,
        });

        self.logger.log(&self.metrics.format_report())?;
        Ok(())
    }

    fn proxy_to_backend(bytes: &Bytes, target_addr: SocketAddr) -> MzaniResult<Bytes>
    {
        let mut backend_stream = TcpStream::connect(target_addr)?;
        backend_stream.set_read_timeout(Some(READ_TIMEOUT))?;
        backend_stream.write_all(bytes)?;
        backend_stream.shutdown(std::net::Shutdown::Write)?;

        let mut backend_reader = std::io::BufReader::new(&backend_stream);
        let mut response_data = Vec::new();
        backend_reader.read_to_end(&mut response_data)?;
        Ok(response_data)
    }

    pub(crate) fn log_error(&self, message: &str)
    {
        let _ = self.logger.log_error(message);
    }
}

/// Acquires a write lock, logging poison errors instead of panicking.
pub(crate) fn write_context(context: &Arc<RwLock<Context>>) -> Option<RwLockWriteGuard<'_, Context>>
{
    if let Ok(guard) = context.write() {
        Some(guard)
    } else {
        if let Ok(guard) = context.read() {
            guard.log_error("shared context lock poisoned");
        }
        None
    }
}

#[cfg(test)]
mod tests
{
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::Context;

    fn test_servers() -> Vec<SocketAddr>
    {
        vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3333),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3334),
        ]
    }

    #[test]
    fn new_rejects_empty_server_list()
    {
        let result = Context::new(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn round_robin_target_selection() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut context = Context::new(test_servers())?;
        let first = context.fetch_target_server();
        let second = context.fetch_target_server();
        let third = context.fetch_target_server();
        assert_eq!(first, test_servers()[0]);
        assert_eq!(second, test_servers()[1]);
        assert_eq!(third, test_servers()[0]);
        Ok(())
    }
}
