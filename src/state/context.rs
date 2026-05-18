use std::fmt;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::time::Instant;

use crate::config::{BalancerConfig, Limits, Timeouts};
use crate::core::http_util::{write_payload_too_large, write_service_unavailable};
use crate::core::request::Request;
use crate::core::response::read_http_response;
use crate::routing::{RoundRobinRouting, RoutingStrategy};
use crate::state::backend::BackendSet;
use crate::state::metrics::{Metrics, RequestOutcome, RequestStats};
use crate::utils::Bytes;
use crate::utils::log_record::{LogLevel, LogRecord, LogRole, RequestContext, format_addr, parse_status_code, path_for_log};
use crate::{MzaniError, MzaniResult};

/// Shared runtime state for the load balancer.
///
/// Holds listen configuration, routing, limits, and handles to
/// asynchronous logging and metrics workers.
pub struct Context {
    socket_addr: SocketAddr,
    log_dir: PathBuf,
    limits: Limits,
    timeouts: Timeouts,
    max_backend_retries: u32,
    routing: Arc<dyn RoutingStrategy>,
    log_tx: Option<SyncSender<LogRecord>>,
    metrics_tx: Option<SyncSender<RequestStats>>,
    metrics: Option<Arc<std::sync::Mutex<Metrics>>>,
    dropped_logs: Option<Arc<AtomicU64>>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("socket_addr", &self.socket_addr)
            .field("log_dir", &self.log_dir)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Context {
    /// Creates a load balancer context from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::EmptyServerList`] if `config.backends` is empty.
    pub fn from_config(config: &BalancerConfig, backends: &Arc<BackendSet>) -> MzaniResult<Self> {
        config.validate()?;
        let routing: Arc<dyn RoutingStrategy> = Arc::new(RoundRobinRouting::new(Arc::clone(backends)));
        Ok(Self {
            socket_addr: config.listen,
            log_dir: config.log_dir.clone(),
            limits: config.limits,
            timeouts: config.timeouts,
            max_backend_retries: config.max_backend_retries,
            routing,
            log_tx: None,
            metrics_tx: None,
            metrics: None,
            dropped_logs: None,
        })
    }

    /// Creates a context with explicit listen and log directories (for integration tests).
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::EmptyServerList`] if `server_list` is empty.
    pub fn new_with_options(
        server_list: Vec<SocketAddr>,
        listen: SocketAddr,
        log_dir: Option<PathBuf>,
    ) -> MzaniResult<Self> {
        if server_list.is_empty() {
            return Err(MzaniError::EmptyServerList);
        }
        let config = BalancerConfig {
            listen,
            backends: server_list,
            log_dir: log_dir.unwrap_or_else(|| PathBuf::from("logs")),
            ..BalancerConfig::default()
        };
        let backends = Arc::new(BackendSet::new(config.backends.clone()));
        Self::from_config(&config, &backends)
    }

    /// Returns the socket address the load balancer listens on.
    #[must_use]
    pub fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    /// Returns backend addresses.
    #[must_use]
    pub fn target_servers(&self) -> Vec<SocketAddr> {
        self.routing.backend_addresses()
    }

    /// Directory where structured logs are written (`mzani.log` inside).
    #[must_use]
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    /// Returns configured HTTP limits.
    #[must_use]
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Returns configured I/O timeouts.
    #[must_use]
    pub fn timeouts(&self) -> Timeouts {
        self.timeouts
    }

    /// Returns a formatted metrics report.
    #[must_use]
    pub fn metrics_report(&self) -> String {
        match self.metrics.as_ref().and_then(|metrics| metrics.lock().ok()) {
            Some(guard) => guard.format_report(),
            None => Metrics::default().format_report(),
        }
    }

    pub(crate) fn attach_observability(
        &mut self,
        log_tx: SyncSender<LogRecord>,
        metrics_tx: SyncSender<RequestStats>,
        metrics: Arc<std::sync::Mutex<Metrics>>,
        dropped_logs: Arc<AtomicU64>,
    ) {
        self.log_tx = Some(log_tx);
        self.metrics_tx = Some(metrics_tx);
        self.metrics = Some(metrics);
        self.dropped_logs = Some(dropped_logs);
    }

    pub(crate) fn detach_observability(&mut self) {
        self.log_tx = None;
        self.metrics_tx = None;
        self.metrics = None;
        self.dropped_logs = None;
    }

    pub(crate) fn emit_log(&self, record: LogRecord) {
        let Some(tx) = &self.log_tx else {
            return;
        };
        if tx.try_send(record).is_err() {
            let Some(counter) = &self.dropped_logs else {
                return;
            };
            let prev = counter.fetch_add(1, Ordering::Relaxed);
            if prev > 0 && prev % 100 == 0 {
                let _ = tx
                    .try_send(LogRecord::new(LogLevel::Warn, "log_dropped", LogRole::Log).field("dropped_count", prev + 1));
            }
        }
    }

    fn record_metrics(&self, stats: RequestStats) {
        if let Some(tx) = &self.metrics_tx {
            let _ = tx.try_send(stats);
        }
    }

    /// Emits a structured accept-loop or pool error.
    pub(crate) fn log_accept_event(&self, event: &'static str, level: LogLevel, fields: &[(&str, String)]) {
        let mut record = LogRecord::new(level, event, LogRole::Accept);
        for (key, value) in fields {
            record = record.field(*key, value.clone());
        }
        self.emit_log(record);
    }

    /// Records a pool rejection in metrics and logs.
    pub(crate) fn record_pool_rejected(&self) {
        if let Some(metrics) = &self.metrics {
            if let Ok(guard) = metrics.lock() {
                guard.record_pool_rejected();
            }
        }
        self.log_accept_event("pool_saturated", LogLevel::Warn, &[]);
    }

    /// Proxies a single client TCP connection to a backend server.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] on I/O, parse, or backend failures.
    #[allow(clippy::too_many_lines)]
    pub fn handle_connection(&self, mut stream: TcpStream, req: RequestContext) -> MzaniResult<()> {
        let start_time = Instant::now();
        stream.set_read_timeout(Some(self.timeouts.read)).map_err(|error| {
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

        let target = match self.routing.select_backend() {
            Ok(addr) => addr,
            Err(MzaniError::NoHealthyBackend) => {
                let _ = write_service_unavailable(&mut stream, "no healthy backends");
                self.log_request_failed(req, start_time, "no_backend", "no healthy backends");
                return Err(MzaniError::NoHealthyBackend);
            }
            Err(error) => return Err(error),
        };

        self.emit_log(
            LogRecord::new(LogLevel::Debug, "backend_selected", LogRole::Worker)
                .with_request(req)
                .field("backend", format_addr(target)),
        );

        let request = match Request::from_stream(&mut stream, &self.limits) {
            Ok(request) => request,
            Err(MzaniError::BodyTooLarge) => {
                let _ = write_payload_too_large(&mut stream);
                self.log_request_failed(req, start_time, "body_limit", "body too large");
                return Err(MzaniError::BodyTooLarge);
            }
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

        let backend_response = match self.proxy_with_retries(&request_bytes, target) {
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

        if duration >= self.timeouts.slow_request_warn {
            self.emit_log(
                LogRecord::new(LogLevel::Warn, "slow_request", LogRole::Worker)
                    .with_request(req)
                    .field("duration_ms", duration.as_millis())
                    .field("threshold_ms", self.timeouts.slow_request_warn.as_millis()),
            );
        }

        Ok(())
    }

    fn proxy_with_retries(&self, bytes: &Bytes, first_target: SocketAddr) -> MzaniResult<Bytes> {
        let mut last_error = None;
        let mut target = first_target;
        let attempts = self.max_backend_retries.saturating_add(1);
        for attempt in 0..attempts {
            match Self::proxy_to_backend(bytes, target, &self.limits, self.timeouts.read) {
                Ok(response) => return Ok(response),
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 >= attempts {
                        break;
                    }
                    target = self.routing.select_backend().unwrap_or(target);
                }
            }
        }
        Err(last_error.unwrap_or(MzaniError::Io("proxy failed".to_owned())))
    }

    fn log_request_failed(&self, req: RequestContext, start: Instant, stage: &'static str, error: impl std::fmt::Display) {
        self.emit_log(
            LogRecord::new(LogLevel::Error, "request_failed", LogRole::Worker)
                .with_request(req)
                .field("stage", stage)
                .field("error", error.to_string())
                .field("duration_ms", start.elapsed().as_millis()),
        );
    }

    fn proxy_to_backend(
        bytes: &Bytes,
        target_addr: SocketAddr,
        limits: &Limits,
        read_timeout: std::time::Duration,
    ) -> MzaniResult<Bytes> {
        let mut backend_stream = TcpStream::connect(target_addr)?;
        backend_stream.set_read_timeout(Some(read_timeout))?;
        backend_stream.write_all(bytes)?;
        read_http_response(&mut backend_stream, limits)
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::Context;
    fn test_servers() -> Vec<SocketAddr> {
        vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3333),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3334),
        ]
    }

    fn test_listen() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000)
    }

    #[test]
    fn new_with_options_rejects_empty_server_list() {
        assert!(Context::new_with_options(vec![], test_listen(), None).is_err());
    }

    #[test]
    fn metrics_report_before_pool_uses_default_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let context = Context::new_with_options(test_servers(), test_listen(), None)?;
        let report = context.metrics_report();
        assert!(report.contains("Total Requests    : 0"));
        Ok(())
    }

    #[test]
    fn observability_unattached_until_pool_starts() -> Result<(), Box<dyn std::error::Error>> {
        let context = Context::new_with_options(test_servers(), test_listen(), None)?;
        assert!(context.metrics_report().contains("Total Requests    : 0"));
        Ok(())
    }
}
