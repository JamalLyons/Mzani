use std::env;
use std::fmt::{self, Display, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::MzaniError;

/// Severity for a structured log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }

    /// Minimum level from `MZANI_LOG` (`debug`, `info`, `warn`, `error`); defaults to `info`.
    pub fn from_env() -> Self {
        match env::var("MZANI_LOG").ok().as_deref().map(str::to_ascii_lowercase) {
            Some(level) if level == "debug" => Self::Debug,
            Some(level) if level == "warn" => Self::Warn,
            Some(level) if level == "error" => Self::Error,
            _ => Self::Info,
        }
    }
}

/// Subsystem that emitted a log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRole {
    Accept,
    Worker,
    Metrics,
    Log,
    Health,
}

impl LogRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Worker => "worker",
            Self::Metrics => "metrics",
            Self::Log => "log",
            Self::Health => "health",
        }
    }
}

/// Correlation identifiers for a single proxied connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestContext {
    /// Monotonic request identifier shared across log lines for one connection.
    pub req_id: u64,
    /// Connection worker index (0..N-1) that handled the request.
    pub worker_id: usize,
}

/// One structured log event (serialized as a single `key=value` line).
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub level: LogLevel,
    pub event: &'static str,
    pub role: LogRole,
    pub req_id: Option<u64>,
    pub worker_id: Option<usize>,
    pub fields: Vec<(String, String)>,
}

impl LogRecord {
    /// Creates a record with the given level, event name, and emitting role.
    #[must_use]
    pub fn new(level: LogLevel, event: &'static str, role: LogRole) -> Self {
        Self {
            level,
            event,
            role,
            req_id: None,
            worker_id: None,
            fields: Vec::new(),
        }
    }

    /// Attaches request and worker correlation IDs.
    #[must_use]
    pub fn with_request(mut self, ctx: RequestContext) -> Self {
        self.req_id = Some(ctx.req_id);
        self.worker_id = Some(ctx.worker_id);
        self
    }

    /// Adds a string field (values containing whitespace are quoted when formatted).
    #[must_use]
    pub fn field(mut self, key: impl Into<String>, value: impl Display) -> Self {
        self.fields.push((key.into(), value.to_string()));
        self
    }

    /// Returns whether this record meets the configured minimum log level.
    #[must_use]
    pub fn meets_min_level(&self, min: LogLevel) -> bool {
        self.level >= min
    }

    /// Formats the record as one grep-friendly line (UTC millisecond timestamp).
    ///
    /// # Panics
    ///
    /// Panics if the system clock is before the Unix epoch.
    #[must_use]
    pub fn format_line(&self) -> String {
        let mut line = String::with_capacity(256);
        let ts_ms = utc_timestamp_ms().unwrap_or(0);
        let _ = write!(line, "ts_ms={ts_ms}");
        let _ = write!(line, " level={}", self.level.as_str());
        let _ = write!(line, " event={}", self.event);
        let _ = write!(line, " role={}", self.role.as_str());
        if let Some(req_id) = self.req_id {
            let _ = write!(line, " req_id={req_id}");
        }
        if let Some(worker_id) = self.worker_id {
            let _ = write!(line, " worker_id={worker_id}");
        }
        for (key, value) in &self.fields {
            let _ = write!(line, " ");
            let _ = write_field(&mut line, key, value);
        }
        line
    }
}

/// Returns path without query string for safer logging.
#[must_use]
pub fn path_for_log(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

/// Parses HTTP status code from the first line of a response buffer.
#[must_use]
pub fn parse_status_code(response: &[u8]) -> Option<u16> {
    let head = std::str::from_utf8(&response[..response.len().min(128)]).ok()?;
    let status_line = head.lines().next()?;
    let mut parts = status_line.split_whitespace();
    parts.next()?;
    parts.next()?.parse().ok()
}

/// Allocates the next monotonic request identifier.
pub fn next_request_id(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, Ordering::Relaxed)
}

fn utc_timestamp_ms() -> Result<u128, MzaniError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MzaniError::InvalidTimestamp)
        .map(|duration| duration.as_millis())
}

fn write_field(line: &mut String, key: &str, value: &str) -> fmt::Result {
    let needs_quotes = value.chars().any(|ch| ch.is_whitespace() || ch == '"' || ch == '=');
    if needs_quotes {
        let escaped = value.replace('"', "\\\"");
        write!(line, "{key}=\"{escaped}\"")
    } else {
        write!(line, "{key}={value}")
    }
}

/// Formats a socket address for log fields.
pub fn format_addr(addr: SocketAddr) -> String {
    addr.to_string()
}

#[cfg(test)]
mod tests {
    use super::{LogLevel, LogRecord, LogRole, RequestContext, parse_status_code, path_for_log, write_field};

    #[test]
    fn format_line_includes_core_fields() {
        let line = LogRecord::new(LogLevel::Info, "request_complete", LogRole::Worker)
            .with_request(RequestContext {
                req_id: 42,
                worker_id: 3,
            })
            .field("method", "GET")
            .format_line();
        assert!(line.contains("event=request_complete"));
        assert!(line.contains("req_id=42"));
        assert!(line.contains("worker_id=3"));
        assert!(line.contains("method=GET"));
    }

    #[test]
    fn quotes_values_with_spaces() -> std::fmt::Result {
        let mut line = String::new();
        write_field(&mut line, "error", "connection reset by peer")?;
        assert_eq!(line, "error=\"connection reset by peer\"");
        Ok(())
    }

    #[test]
    fn path_for_log_strips_query() {
        assert_eq!(path_for_log("/api/health?token=secret"), "/api/health");
    }

    #[test]
    fn parse_status_code_from_response() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(parse_status_code(raw), Some(200));
    }

    #[test]
    fn log_level_ordering() {
        assert!(LogLevel::Warn >= LogLevel::Info);
        assert!(LogLevel::Debug < LogLevel::Error);
    }
}
