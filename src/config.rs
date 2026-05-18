//! Load balancer configuration types and validation.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::{MzaniError, MzaniResult};

/// Maximum sizes for HTTP messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum HTTP header block size in bytes.
    pub max_header_bytes: usize,
    /// Maximum request body size in bytes.
    pub max_body_bytes: usize,
    /// Maximum decoded response body size in bytes.
    pub max_response_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_header_bytes: 64 * 1024,
            max_body_bytes: 16 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Limits {
    /// Validates limit invariants.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] if any limit is zero.
    pub fn validate(&self) -> MzaniResult<()> {
        if self.max_header_bytes == 0 || self.max_body_bytes == 0 || self.max_response_bytes == 0 {
            return Err(MzaniError::ParseError("limits must be greater than zero".to_owned()));
        }
        Ok(())
    }
}

/// Read and connect timeouts for proxied traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// Client and backend read timeout.
    pub read: Duration,
    /// Threshold for slow-request warning logs.
    pub slow_request_warn: Duration,
    /// Accept-loop client read timeout.
    pub accept_read: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            read: Duration::from_secs(30),
            slow_request_warn: Duration::from_millis(500),
            accept_read: Duration::from_secs(30),
        }
    }
}

/// TCP connect probe settings for backend health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthConfig {
    /// Interval between probe rounds across all backends.
    pub interval: Duration,
    /// Per-backend TCP connect timeout.
    pub connect_timeout: Duration,
    /// Consecutive failures before marking a backend unhealthy.
    pub failure_threshold: u32,
    /// Consecutive successes before marking a backend healthy again.
    pub recovery_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            failure_threshold: 2,
            recovery_threshold: 1,
        }
    }
}

impl HealthConfig {
    /// Validates health configuration.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] on invalid thresholds or zero durations.
    pub fn validate(&self) -> MzaniResult<()> {
        if self.interval.is_zero() || self.connect_timeout.is_zero() {
            return Err(MzaniError::ParseError("health intervals must be non-zero".to_owned()));
        }
        if self.failure_threshold == 0 || self.recovery_threshold == 0 {
            return Err(MzaniError::ParseError("health thresholds must be at least 1".to_owned()));
        }
        Ok(())
    }
}

/// Thread pool and channel sizing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// Number of connection worker threads.
    pub workers: usize,
    /// Per-worker connection queue capacity.
    pub per_worker_queue: usize,
    /// Bounded log channel capacity.
    pub log_channel_capacity: usize,
    /// Bounded metrics channel capacity.
    pub metrics_channel_capacity: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: 10,
            per_worker_queue: 7,
            log_channel_capacity: 1024,
            metrics_channel_capacity: 1024,
        }
    }
}

impl PoolConfig {
    /// Validates pool configuration.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ParseError`] if workers or queue sizes are zero.
    pub fn validate(&self) -> MzaniResult<()> {
        if self.workers == 0 || self.per_worker_queue == 0 {
            return Err(MzaniError::ParseError(
                "workers and per_worker_queue must be at least 1".to_owned(),
            ));
        }
        if self.log_channel_capacity == 0 || self.metrics_channel_capacity == 0 {
            return Err(MzaniError::ParseError("channel capacities must be at least 1".to_owned()));
        }
        Ok(())
    }
}

/// Full configuration for [`crate::Balancer`].
#[derive(Debug, Clone)]
pub struct BalancerConfig {
    /// Address the load balancer listens on.
    pub listen: SocketAddr,
    /// Backend server addresses (at least one).
    pub backends: Vec<SocketAddr>,
    /// Directory for structured log files.
    pub log_dir: PathBuf,
    /// HTTP and response size limits.
    pub limits: Limits,
    /// I/O timeouts.
    pub timeouts: Timeouts,
    /// Backend health probe settings.
    pub health: HealthConfig,
    /// Worker pool and channel sizing.
    pub pool: PoolConfig,
    /// Maximum proxy retries to other healthy backends on connect failure.
    pub max_backend_retries: u32,
}

impl Default for BalancerConfig {
    fn default() -> Self {
        Self {
            listen: std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080),
            backends: Vec::new(),
            log_dir: PathBuf::from("logs"),
            limits: Limits::default(),
            timeouts: Timeouts::default(),
            health: HealthConfig::default(),
            pool: PoolConfig::default(),
            max_backend_retries: 1,
        }
    }
}

impl BalancerConfig {
    /// Validates the full configuration.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::EmptyServerList`] if `backends` is empty.
    /// Returns [`MzaniError::ParseError`] for invalid nested config.
    pub fn validate(&self) -> MzaniResult<()> {
        if self.backends.is_empty() {
            return Err(MzaniError::EmptyServerList);
        }
        self.limits.validate()?;
        self.health.validate()?;
        self.pool.validate()?;
        Ok(())
    }
}
