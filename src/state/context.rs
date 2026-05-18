use std::env;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};

use crate::core::request::Request;
use crate::core::response::read_http_response;
use crate::state::metrics::{Metrics, RequestOutcome, RequestStats};
use crate::utils::Bytes;
use crate::utils::log_record::{LogLevel, LogRecord, LogRole, RequestContext, format_addr, parse_status_code, path_for_log};
use crate::{MzaniError, MzaniResult};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:5000";
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const SLOW_REQUEST_THRESHOLD: Duration = Duration::from_millis(500);

/// Shared runtime state for the load balancer.
///
/// Holds listen configuration, round-robin backend selection, and handles to
/// asynchronous logging and metrics workers.
#[derive(Debug)]
pub struct Context
{
    idx: usize,
    socket_addr: SocketAddr,
    target_servers: Vec<SocketAddr>,
    log_tx: Option<Sender<LogRecord>>,
    metrics_tx: Option<Sender<RequestStats>>,
    metrics: Option<Arc<Mutex<Metrics>>>,
    next_req_id: Option<Arc<AtomicU64>>,
    dropped_logs: Option<Arc<AtomicU64>>,
}

impl Context
{
    /// Creates a load balancer context with the given backend servers.
    ///
    /// Logging and metrics channels are attached when a [`crate::core::pool::ThreadPool`]
    /// is constructed for this context.
    ///
    /// # Arguments
    ///
    /// * `server_list` - Non-empty list of backend socket addresses
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::EmptyServerList`] if `server_list` is empty.
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
            log_tx: None,
            metrics_tx: None,
            metrics: None,
            next_req_id: None,
            dropped_logs: None,
        })
    }

    /// Returns the socket address the load balancer listens on.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr
    {
        self.socket_addr
    }

    /// Returns backend addresses for startup logging.
    #[must_use]
    pub fn target_servers(&self) -> &[SocketAddr]
    {
        &self.target_servers
    }

    /// Returns a formatted metrics report.
    #[must_use]
    pub fn metrics_report(&self) -> String
    {
        match self.metrics.as_ref().and_then(|metrics| metrics.lock().ok()) {
            Some(guard) => guard.format_report(),
            None => Metrics::default().format_report(),
        }
    }

    pub(crate) fn attach_observability(
        &mut self,
        log_tx: Sender<LogRecord>,
        metrics_tx: Sender<RequestStats>,
        metrics: Arc<Mutex<Metrics>>,
        next_req_id: Arc<AtomicU64>,
        dropped_logs: Arc<AtomicU64>,
    )
    {
        self.log_tx = Some(log_tx);
        self.metrics_tx = Some(metrics_tx);
        self.metrics = Some(metrics);
        self.next_req_id = Some(next_req_id);
        self.dropped_logs = Some(dropped_logs);
    }

    pub(crate) fn detach_observability(&mut self)
    {
        self.log_tx = None;
        self.metrics_tx = None;
        self.metrics = None;
        self.next_req_id = None;
        self.dropped_logs = None;
    }

    #[cfg(test)]
    pub(crate) fn has_observability(&self) -> bool
    {
        self.log_tx.is_some() && self.metrics_tx.is_some() && self.metrics.is_some()
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

    pub(crate) fn emit_log(&self, record: LogRecord)
    {
        let Some(tx) = &self.log_tx else {
            return;
        };
        if tx.send(record).is_err()
            && let Some(counter) = &self.dropped_logs
        {
            let prev = counter.fetch_add(1, Ordering::Relaxed);
            if prev > 0 && prev % 100 == 0 {
                let _ =
                    tx.send(LogRecord::new(LogLevel::Warn, "log_dropped", LogRole::Log).field("dropped_count", prev + 1));
            }
        }
    }

    fn record_metrics(&self, stats: RequestStats)
    {
        if let Some(tx) = &self.metrics_tx {
            let _ = tx.send(stats);
        }
    }

    /// Emits a structured accept-loop or pool error.
    pub(crate) fn log_accept_event(&self, event: &'static str, level: LogLevel, fields: &[(&str, String)])
    {
        let mut record = LogRecord::new(level, event, LogRole::Accept);
        for (key, value) in fields {
            record = record.field(*key, value.clone());
        }
        self.emit_log(record);
    }

    /// Proxies a single client TCP connection to a backend server.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] on I/O, parse, or backend failures.
    #[allow(clippy::too_many_lines)]
    pub fn handle_connection(&mut self, mut stream: TcpStream, req: RequestContext) -> MzaniResult<()>
    {
        let start_time = Instant::now();
        stream.set_read_timeout(Some(READ_TIMEOUT)).map_err(|error| {
            let err = MzaniError::from(error);
            self.log_request_failed(req, start_time, "set_timeout", &err);
            err
        })?;

        let client = stream.peer_addr().map_or_else(|_| "-".to_owned(), format_addr);

        self.emit_log(
            LogRecord::new(LogLevel::Debug, "connection_accepted", LogRole::Worker)
                .with_request(req)
                .field("client", client)
                .field("listen", format_addr(self.socket_addr)),
        );

        let target = self.fetch_target_server();
        self.emit_log(
            LogRecord::new(LogLevel::Debug, "backend_selected", LogRole::Worker)
                .with_request(req)
                .field("backend", format_addr(target)),
        );

        let request = match Request::from_stream(&mut stream) {
            Ok(request) => request,
            Err(error) => {
                self.log_request_failed(req, start_time, "parse", &error);
                return Err(error);
            }
        };

        let request_bytes = request.to_bytes();
        let request_len = request_bytes.len();
        let content_length = request.body().len();
        let host = request
            .headers()
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("host"))
            .map_or("-", |(_, value)| value.as_str());
        let user_agent = header_value(request.headers(), "user-agent");
        let x_request_id = header_value(request.headers(), "x-request-id");

        let mut parsed = LogRecord::new(LogLevel::Info, "request_parsed", LogRole::Worker)
            .with_request(req)
            .field("method", request.method().as_str())
            .field("path", path_for_log(request.path()))
            .field("host", host)
            .field("content_length", content_length)
            .field("req_bytes", request_len);
        if let Some(ua) = user_agent {
            parsed = parsed.field("user_agent", ua);
        }
        if let Some(rid) = x_request_id {
            parsed = parsed.field("x_request_id", rid);
        }
        self.emit_log(parsed);

        self.emit_log(
            LogRecord::new(LogLevel::Debug, "backend_connect", LogRole::Worker)
                .with_request(req)
                .field("backend", format_addr(target)),
        );

        let backend_response = match Self::proxy_to_backend(&request_bytes, target) {
            Ok(response) => response,
            Err(error) => {
                self.log_request_failed(req, start_time, "proxy", &error);
                self.record_metrics(RequestStats {
                    req_id: req.req_id,
                    worker_id: req.worker_id,
                    backend: target,
                    request_len,
                    response_len: 0,
                    duration: start_time.elapsed(),
                    outcome: RequestOutcome::Error,
                    status_code: None,
                });
                return Err(error);
            }
        };

        let response_len = backend_response.len();
        let status_code = parse_status_code(&backend_response);

        stream.write_all(&backend_response).map_err(|error| {
            let err = MzaniError::from(error);
            self.log_request_failed(req, start_time, "client_write", &err);
            err
        })?;
        stream.flush().map_err(|error| {
            let err = MzaniError::from(error);
            self.log_request_failed(req, start_time, "client_flush", &err);
            err
        })?;
        stream.shutdown(std::net::Shutdown::Write).map_err(|error| {
            let err = MzaniError::from(error);
            self.log_request_failed(req, start_time, "client_shutdown", &err);
            err
        })?;

        let duration = start_time.elapsed();
        self.record_metrics(RequestStats {
            req_id: req.req_id,
            worker_id: req.worker_id,
            backend: target,
            request_len,
            response_len,
            duration,
            outcome: RequestOutcome::Ok,
            status_code,
        });

        let mut complete = LogRecord::new(LogLevel::Info, "request_complete", LogRole::Worker)
            .with_request(req)
            .field("backend", format_addr(target))
            .field("req_bytes", request_len)
            .field("resp_bytes", response_len)
            .field("duration_ms", duration.as_millis())
            .field("outcome", "ok");
        if let Some(status) = status_code {
            complete = complete.field("status", status);
        }
        self.emit_log(complete);

        if duration >= SLOW_REQUEST_THRESHOLD {
            self.emit_log(
                LogRecord::new(LogLevel::Warn, "slow_request", LogRole::Worker)
                    .with_request(req)
                    .field("duration_ms", duration.as_millis())
                    .field("threshold_ms", SLOW_REQUEST_THRESHOLD.as_millis()),
            );
        }

        Ok(())
    }

    fn log_request_failed(&self, req: RequestContext, start: Instant, stage: &'static str, error: impl std::fmt::Display)
    {
        self.emit_log(
            LogRecord::new(LogLevel::Error, "request_failed", LogRole::Worker)
                .with_request(req)
                .field("stage", stage)
                .field("error", error.to_string())
                .field("duration_ms", start.elapsed().as_millis()),
        );
    }

    fn proxy_to_backend(bytes: &Bytes, target_addr: SocketAddr) -> MzaniResult<Bytes>
    {
        let mut backend_stream = TcpStream::connect(target_addr)?;
        backend_stream.set_read_timeout(Some(READ_TIMEOUT))?;
        backend_stream.write_all(bytes)?;
        read_http_response(&mut backend_stream)
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str>
{
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Acquires a write lock, logging poison errors instead of panicking.
pub(crate) fn write_context(context: &Arc<RwLock<Context>>) -> Option<RwLockWriteGuard<'_, Context>>
{
    if let Ok(guard) = context.write() {
        Some(guard)
    } else {
        if let Ok(guard) = context.read() {
            guard.emit_log(LogRecord::new(LogLevel::Error, "context_lock_poisoned", LogRole::Worker));
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

    #[test]
    fn metrics_report_before_pool_uses_default_snapshot() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = Context::new(test_servers())?;
        let report = context.metrics_report();
        assert!(report.contains("Total Requests    : 0"));
        Ok(())
    }

    #[test]
    fn observability_unattached_until_pool_starts() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = Context::new(test_servers())?;
        assert!(!context.has_observability());
        Ok(())
    }
}
