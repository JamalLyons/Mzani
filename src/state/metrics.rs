use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::utils::log_record::{LogLevel, LogRecord, LogRole, RequestContext, format_addr};

/// Outcome of a proxied request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// Request proxied successfully.
    Ok,
    /// Request failed during proxying.
    Error,
}

impl RequestOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

/// Per-request statistics passed to the metrics aggregator thread.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestStats {
    pub req_id: u64,
    pub worker_id: usize,
    pub backend: SocketAddr,
    pub request_len: usize,
    pub response_len: usize,
    pub duration: Duration,
    pub outcome: RequestOutcome,
    pub status_code: Option<u16>,
}

impl RequestStats {
    /// Builds a compact structured log record for the metrics thread.
    #[must_use]
    pub fn to_metrics_record(self, req: RequestContext) -> LogRecord {
        let mut record = LogRecord::new(LogLevel::Info, "metrics_record", LogRole::Metrics)
            .with_request(req)
            .field("backend", format_addr(self.backend))
            .field("req_bytes", self.request_len)
            .field("resp_bytes", self.response_len)
            .field("duration_ms", self.duration.as_millis())
            .field("outcome", self.outcome.as_str());
        if let Some(status) = self.status_code {
            record = record.field("status", status);
        }
        record
    }
}

/// Per-backend request counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct BackendMetrics {
    /// Total proxied requests to this backend.
    pub requests: u64,
    /// Failed proxied requests.
    pub errors: u64,
}

/// Point-in-time load balancer metrics for operators and embedders.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total completed proxied requests.
    pub total_requests: u64,
    /// Total bytes proxied (request + response).
    pub total_bytes_proxied: u64,
    /// Connections rejected because worker queues were full.
    pub pool_rejected: u64,
    /// Log records dropped because the log channel was full.
    pub dropped_logs: u64,
    /// Average request duration over all completed requests.
    pub avg_latency: Duration,
    /// Duration of the most recent completed request.
    pub last_request_duration: Duration,
    /// Per-backend breakdown.
    pub per_backend: HashMap<SocketAddr, BackendMetrics>,
}

/// Aggregated load balancer metrics.
#[derive(Debug)]
pub(crate) struct Metrics {
    total_requests: u64,
    total_bytes_proxied: u64,
    largest_request_bytes: usize,
    last_request_duration: Duration,
    total_duration: Duration,
    server_start_time: Instant,
    per_backend: HashMap<SocketAddr, BackendMetrics>,
    pool_rejected: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            total_requests: 0,
            total_bytes_proxied: 0,
            largest_request_bytes: 0,
            last_request_duration: Duration::default(),
            total_duration: Duration::default(),
            server_start_time: Instant::now(),
            per_backend: HashMap::new(),
            pool_rejected: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    /// Records metrics for a completed proxied request.
    pub fn record_request(&mut self, stats: RequestStats) {
        self.total_requests += 1;
        self.total_bytes_proxied += (stats.request_len + stats.response_len) as u64;
        self.total_duration += stats.duration;
        self.last_request_duration = stats.duration;
        if stats.request_len > self.largest_request_bytes {
            self.largest_request_bytes = stats.request_len;
        }

        let entry = self.per_backend.entry(stats.backend).or_default();
        entry.requests += 1;
        if stats.outcome == RequestOutcome::Error {
            entry.errors += 1;
        }
    }

    /// Increments the count of pool rejections.
    pub fn record_pool_rejected(&self) {
        self.pool_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Builds a point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self, dropped_logs: u64) -> MetricsSnapshot {
        let avg_latency = if self.total_requests > 0 {
            let divisor = u32::try_from(self.total_requests).unwrap_or(1);
            self.total_duration / divisor
        } else {
            Duration::default()
        };

        MetricsSnapshot {
            total_requests: self.total_requests,
            total_bytes_proxied: self.total_bytes_proxied,
            pool_rejected: self.pool_rejected.load(Ordering::Relaxed),
            dropped_logs,
            avg_latency,
            last_request_duration: self.last_request_duration,
            per_backend: self.per_backend.clone(),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn format_bytes(bytes: u64) -> String {
        const KB: f64 = 1024.0;
        const MB: f64 = KB * 1024.0;
        const GB: f64 = MB * 1024.0;
        let bytes_f64 = bytes as f64;

        if bytes_f64 >= GB {
            format!("{:.2} GB", bytes_f64 / GB)
        } else if bytes_f64 >= MB {
            format!("{:.2} MB", bytes_f64 / MB)
        } else if bytes_f64 >= KB {
            format!("{:.2} KB", bytes_f64 / KB)
        } else {
            format!("{bytes} bytes")
        }
    }

    /// Builds a human-readable metrics report.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn format_report(&self) -> String {
        let avg_latency = if self.total_requests > 0 {
            let divisor = u32::try_from(self.total_requests).unwrap_or(1);
            self.total_duration / divisor
        } else {
            Duration::default()
        };

        let uptime = self.server_start_time.elapsed().as_secs_f64();
        let rps = if uptime > 0.0 {
            self.total_requests as f64 / uptime
        } else {
            0.0
        };

        let mut report = format!(
            "--- [ LOAD BALANCER METRICS ] ---\n\
             Uptime            : {uptime:.2}s\n\
             Total Requests    : {}\n\
             Pool Rejected     : {}\n\
             Throughput        : {}\n\
             Avg Latency       : {avg_latency:?}\n\
             Last Req Duration : {:?}\n\
             Largest Payload   : {}\n\
             Requests / Sec    : {rps:.2}\n",
            self.total_requests,
            self.pool_rejected.load(Ordering::Relaxed),
            Self::format_bytes(self.total_bytes_proxied),
            self.last_request_duration,
            Self::format_bytes(self.largest_request_bytes as u64),
        );

        if !self.per_backend.is_empty() {
            report.push_str(" Per-Backend:\n");
            for (addr, stats) in &self.per_backend {
                report.push_str(&format!(
                    "   {} requests={} errors={}\n",
                    format_addr(*addr),
                    stats.requests,
                    stats.errors
                ));
            }
        }
        report.push_str(" ---------------------------------");
        report
    }

    /// Structured log record for periodic aggregate snapshots.
    #[must_use]
    pub fn snapshot_record(&self) -> LogRecord {
        LogRecord::new(LogLevel::Info, "metrics_snapshot", LogRole::Metrics)
            .field("total_requests", self.total_requests)
            .field("report", self.format_report())
    }
}

impl fmt::Display for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_report())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use super::{Metrics, RequestOutcome, RequestStats};

    fn sample_stats() -> RequestStats {
        RequestStats {
            req_id: 1,
            worker_id: 0,
            backend: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            request_len: 100,
            response_len: 200,
            duration: Duration::from_millis(10),
            outcome: RequestOutcome::Ok,
            status_code: Some(200),
        }
    }

    #[test]
    fn format_bytes_uses_kilobytes() {
        assert_eq!(Metrics::format_bytes(2048), "2.00 KB");
    }

    #[test]
    fn record_request_updates_totals() {
        let mut metrics = Metrics::default();
        metrics.record_request(sample_stats());
        let report = metrics.format_report();
        assert!(report.contains("Total Requests    : 1"));
        assert!(report.contains("300"));
    }

    #[test]
    fn format_report_handles_zero_requests() {
        let metrics = Metrics::default();
        let report = metrics.format_report();
        assert!(report.contains("Total Requests    : 0"));
    }

    #[test]
    fn snapshot_record_has_event_name() {
        let metrics = Metrics::default();
        let record = metrics.snapshot_record();
        assert_eq!(record.event, "metrics_snapshot");
    }

    #[test]
    fn snapshot_includes_pool_rejected() {
        let metrics = Metrics::default();
        metrics.record_pool_rejected();
        let snap = metrics.snapshot(0);
        assert_eq!(snap.pool_rejected, 1);
    }
}
