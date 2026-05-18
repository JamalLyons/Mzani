use std::fmt;
use std::time::{Duration, Instant};

/// Per-request statistics passed to [`Metrics::record_request`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestStats
{
    pub request_len: usize,
    pub response_len: usize,
    pub duration: Duration,
}

/// Aggregated load balancer metrics.
#[derive(Debug)]
pub(crate) struct Metrics
{
    total_requests: u64,
    total_bytes_proxied: u64,
    largest_request_bytes: usize,
    last_request_duration: Duration,
    total_duration: Duration,
    server_start_time: Instant,
}

impl Default for Metrics
{
    fn default() -> Self
    {
        Self {
            total_requests: 0,
            total_bytes_proxied: 0,
            largest_request_bytes: 0,
            last_request_duration: Duration::default(),
            total_duration: Duration::default(),
            server_start_time: Instant::now(),
        }
    }
}

impl Metrics
{
    /// Records metrics for a completed proxied request.
    pub fn record_request(&mut self, stats: RequestStats)
    {
        self.total_requests += 1;
        self.total_bytes_proxied += (stats.request_len + stats.response_len) as u64;
        self.total_duration += stats.duration;
        self.last_request_duration = stats.duration;
        if stats.request_len > self.largest_request_bytes {
            self.largest_request_bytes = stats.request_len;
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn format_bytes(bytes: u64) -> String
    {
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
    pub fn format_report(&self) -> String
    {
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

        format!(
            "--- [ LOAD BALANCER METRICS ] ---\n\
             Uptime            : {uptime:.2}s\n\
             Total Requests    : {}\n\
             Throughput        : {}\n\
             Avg Latency       : {avg_latency:?}\n\
             Last Req Duration : {:?}\n\
             Largest Payload   : {}\n\
             Requests / Sec    : {rps:.2}\n\
             ---------------------------------",
            self.total_requests,
            Self::format_bytes(self.total_bytes_proxied),
            self.last_request_duration,
            Self::format_bytes(self.largest_request_bytes as u64),
        )
    }
}

impl fmt::Display for Metrics
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        write!(f, "{}", self.format_report())
    }
}

#[cfg(test)]
mod tests
{
    use std::time::Duration;

    use super::{Metrics, RequestStats};

    #[test]
    fn format_bytes_uses_kilobytes()
    {
        assert_eq!(Metrics::format_bytes(2048), "2.00 KB");
    }

    #[test]
    fn record_request_updates_totals()
    {
        let mut metrics = Metrics::default();
        metrics.record_request(RequestStats {
            request_len: 100,
            response_len: 200,
            duration: Duration::from_millis(10),
        });
        let report = metrics.format_report();
        assert!(report.contains("Total Requests    : 1"));
        assert!(report.contains("300"));
    }

    #[test]
    fn format_report_handles_zero_requests()
    {
        let metrics = Metrics::default();
        let report = metrics.format_report();
        assert!(report.contains("Total Requests    : 0"));
    }
}
