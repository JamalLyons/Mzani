//! Background TCP health probes for backends.

use std::fmt;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::config::HealthConfig;
use crate::state::backend::BackendSet;
use crate::utils::log_record::{LogLevel, LogRecord, LogRole};
use crate::utils::logger::LogSink;

/// Handle to a running health probe thread.
pub struct HealthMonitor {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HealthMonitor {
    /// Spawns a background thread that probes backends on an interval.
    #[must_use]
    pub fn start(backends: Arc<BackendSet>, config: HealthConfig, log: Arc<dyn LogSink>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::spawn(move || health_loop(&stop_flag, &backends, config, &log));
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Signals the probe thread to stop and waits for it to exit.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

impl fmt::Debug for HealthMonitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HealthMonitor").finish_non_exhaustive()
    }
}

fn health_loop(stop: &AtomicBool, backends: &BackendSet, config: HealthConfig, log: &dyn LogSink) {
    let addrs = backends.addresses();
    while !stop.load(Ordering::Relaxed) {
        for addr in &addrs {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let was_healthy = backends.is_healthy(*addr);
            let probe_ok = tcp_probe(*addr, config.connect_timeout);
            if probe_ok {
                backends.record_probe_success(*addr, config.recovery_threshold);
                if !was_healthy && backends.is_healthy(*addr) {
                    log.emit(
                        LogRecord::new(LogLevel::Info, "backend_recovered", LogRole::Health)
                            .field("backend", addr.to_string()),
                    );
                }
            } else {
                backends.record_probe_failure(*addr, config.failure_threshold);
                if was_healthy && !backends.is_healthy(*addr) {
                    log.emit(
                        LogRecord::new(LogLevel::Warn, "backend_unhealthy", LogRole::Health)
                            .field("backend", addr.to_string()),
                    );
                }
            }
        }
        sleep_interruptible(config.interval, stop);
    }
}

fn tcp_probe(addr: SocketAddr, timeout: Duration) -> bool {
    TcpStream::connect_timeout(&addr, timeout).is_ok()
}

fn sleep_interruptible(total: Duration, stop: &AtomicBool) {
    const SLICE: Duration = Duration::from_millis(50);
    let mut remaining = total;
    while remaining > Duration::ZERO && !stop.load(Ordering::Relaxed) {
        let slice = remaining.min(SLICE);
        thread::sleep(slice);
        remaining = remaining.saturating_sub(slice);
    }
}
