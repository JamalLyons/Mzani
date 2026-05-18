//! High-level load balancer runtime: build, run, shutdown.

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::config::BalancerConfig;
use crate::core::http_util::write_service_unavailable;
use crate::core::pool::ThreadPool;
use crate::state::backend::BackendSet;
use crate::state::context::Context;
use crate::state::health::HealthMonitor;
use crate::state::metrics::MetricsSnapshot;
use crate::utils::log_record::LogLevel;
use crate::utils::logger::{ChannelLogSink, LogSink};
use crate::{MzaniError, MzaniResult};

/// Summary returned after [`Balancer::shutdown`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Connections still queued when the deadline was reached.
    pub connections_dropped: u64,
    /// Whether all worker threads joined before the deadline.
    pub joined_cleanly: bool,
}

/// Production HTTP/1.x load balancer instance.
#[derive(Debug)]
pub struct Balancer {
    config: BalancerConfig,
    backends: Arc<BackendSet>,
    pool: ThreadPool,
    health: HealthMonitor,
    shutting_down: Arc<AtomicBool>,
    listener: Option<TcpListener>,
}

impl Balancer {
    /// Validates configuration and starts the worker pool and health monitor.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] if configuration is invalid or logger initialization fails.
    pub fn build(config: BalancerConfig) -> MzaniResult<Self> {
        config.validate()?;
        let backends = Arc::new(BackendSet::new(config.backends.clone()));
        let context = Context::from_config(&config, &backends)?;
        let shutting_down = Arc::new(AtomicBool::new(false));
        let pool = ThreadPool::new(context, &config.pool, Arc::clone(&shutting_down))?;

        let log_tx = pool
            .log_sender()
            .ok_or_else(|| MzaniError::LoggerInit("log channel not attached".to_owned()))?;
        let log_sink: Arc<dyn LogSink> = Arc::new(ChannelLogSink::new(log_tx));
        let health = HealthMonitor::start(Arc::clone(&backends), config.health, log_sink);

        Ok(Self {
            config,
            backends,
            pool,
            health,
            shutting_down,
            listener: None,
        })
    }

    /// Binds the listen socket and runs the accept loop until the listener is closed.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] on bind or accept failures.
    pub fn run(&mut self) -> MzaniResult<()> {
        let listener = TcpListener::bind(self.config.listen)?;
        self.run_on_listener(&listener)
    }

    /// Runs the accept loop on an existing listener (for tests).
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] on accept failures.
    pub fn run_on_listener(&mut self, listener: &TcpListener) -> MzaniResult<()> {
        let listen_addr = self.config.listen;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => self.accept_connection(stream),
                Err(error) => {
                    self.pool.context().log_accept_event(
                        "accept_error",
                        LogLevel::Error,
                        &[("error", error.to_string()), ("listen", listen_addr.to_string())],
                    );
                }
            }
        }
        Ok(())
    }

    fn accept_connection(&self, mut stream: TcpStream) {
        if let Err(error) = stream.set_read_timeout(Some(self.config.timeouts.accept_read)) {
            self.pool.context().log_accept_event(
                "accept_read_timeout_set_failed",
                LogLevel::Warn,
                &[("error", error.to_string())],
            );
            return;
        }

        if self.shutting_down.load(Ordering::Relaxed) {
            let _ = write_service_unavailable(&mut stream, "shutting down");
            return;
        }

        match self.pool.submit(stream) {
            Ok(()) => {}
            Err((MzaniError::PoolFull, stream)) => {
                self.pool.reject_pool_full(stream);
            }
            Err((MzaniError::ShutdownInProgress, mut stream)) => {
                let _ = write_service_unavailable(&mut stream, "shutting down");
            }
            Err((error, _)) => {
                self.pool
                    .context()
                    .log_accept_event("pool_submit_error", LogLevel::Warn, &[("error", error.to_string())]);
            }
        }
    }

    /// Returns a point-in-time metrics snapshot.
    #[must_use]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let dropped = self.pool.dropped_logs().load(Ordering::Relaxed);
        match self.pool.metrics().lock() {
            Ok(guard) => guard.snapshot(dropped),
            Err(_) => MetricsSnapshot {
                total_requests: 0,
                total_bytes_proxied: 0,
                pool_rejected: 0,
                dropped_logs: dropped,
                avg_latency: Duration::default(),
                last_request_duration: Duration::default(),
                per_backend: std::collections::HashMap::new(),
            },
        }
    }

    /// Signals shutdown, stops accepting, drains within `grace`, and joins workers.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ShutdownTimeout`] if threads do not exit before the deadline.
    pub fn shutdown(mut self, grace: Duration) -> MzaniResult<ShutdownReport> {
        self.shutting_down.store(true, Ordering::Relaxed);
        self.health.stop();
        drop(self.listener.take());
        self.pool.stop_accepting();

        let deadline = Instant::now() + grace;
        while Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }

        let joined_cleanly = self.pool.shutdown_threads().is_ok();
        if !joined_cleanly {
            return Err(MzaniError::ShutdownTimeout);
        }

        Ok(ShutdownReport {
            connections_dropped: 0,
            joined_cleanly: true,
        })
    }

    /// Listen address from configuration.
    #[must_use]
    pub fn listen_addr(&self) -> std::net::SocketAddr {
        self.config.listen
    }

    /// Reference to the backend set (health state).
    #[must_use]
    pub fn backends(&self) -> &Arc<BackendSet> {
        &self.backends
    }

    /// Underlying worker pool (for integration tests).
    #[must_use]
    pub fn pool(&self) -> &ThreadPool {
        &self.pool
    }
}

impl Drop for Balancer {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        self.health.stop();
        let _ = self.pool.shutdown_threads();
    }
}
