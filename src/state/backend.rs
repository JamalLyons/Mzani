//! Backend registry with round-robin selection and health state.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::{MzaniError, MzaniResult};

/// Per-backend health and address.
#[derive(Debug)]
struct BackendEntry {
    addr: SocketAddr,
    healthy: AtomicBool,
    consecutive_failures: std::sync::Mutex<u32>,
    consecutive_successes: std::sync::Mutex<u32>,
}

/// Thread-safe backend set with round-robin selection skipping unhealthy nodes.
#[derive(Debug)]
pub struct BackendSet {
    entries: Arc<[BackendEntry]>,
    next_index: AtomicUsize,
}

impl BackendSet {
    /// Creates a set with all backends initially marked healthy.
    #[must_use]
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        let entries: Arc<[BackendEntry]> = addrs
            .into_iter()
            .map(|addr| BackendEntry {
                addr,
                healthy: AtomicBool::new(true),
                consecutive_failures: std::sync::Mutex::new(0),
                consecutive_successes: std::sync::Mutex::new(0),
            })
            .collect();
        Self {
            entries,
            next_index: AtomicUsize::new(0),
        }
    }

    /// Returns all configured backend addresses.
    #[must_use]
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.entries.iter().map(|entry| entry.addr).collect()
    }

    /// Number of backends currently marked healthy.
    #[must_use]
    pub fn healthy_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.healthy.load(Ordering::Relaxed))
            .count()
    }

    /// Selects the next healthy backend in round-robin order.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::NoHealthyBackend`] if every backend is unhealthy.
    pub fn select_next(&self) -> MzaniResult<SocketAddr> {
        let len = self.entries.len();
        if len == 0 {
            return Err(MzaniError::EmptyServerList);
        }

        let start = self.next_index.fetch_add(1, Ordering::Relaxed);
        for offset in 0..len {
            let index = (start + offset) % len;
            let entry = &self.entries[index];
            if entry.healthy.load(Ordering::Relaxed) {
                return Ok(entry.addr);
            }
        }
        Err(MzaniError::NoHealthyBackend)
    }

    /// Records a successful health probe for `addr`.
    pub fn record_probe_success(&self, addr: SocketAddr, recovery_threshold: u32) {
        let Some(entry) = self.entry_for(addr) else {
            return;
        };
        if let Ok(mut failures) = entry.consecutive_failures.lock() {
            *failures = 0;
        }
        let successes = {
            let Ok(mut count) = entry.consecutive_successes.lock() else {
                return;
            };
            *count = count.saturating_add(1);
            *count
        };
        if successes >= recovery_threshold {
            entry.healthy.store(true, Ordering::Relaxed);
        }
    }

    /// Records a failed health probe for `addr`.
    pub fn record_probe_failure(&self, addr: SocketAddr, failure_threshold: u32) {
        let Some(entry) = self.entry_for(addr) else {
            return;
        };
        if let Ok(mut successes) = entry.consecutive_successes.lock() {
            *successes = 0;
        }
        let failures = {
            let Ok(mut count) = entry.consecutive_failures.lock() else {
                return;
            };
            *count = count.saturating_add(1);
            *count
        };
        if failures >= failure_threshold {
            entry.healthy.store(false, Ordering::Relaxed);
        }
    }

    /// Returns whether `addr` is currently marked healthy.
    #[must_use]
    pub fn is_healthy(&self, addr: SocketAddr) -> bool {
        self.entry_for(addr)
            .is_some_and(|entry| entry.healthy.load(Ordering::Relaxed))
    }

    fn entry_for(&self, addr: SocketAddr) -> Option<&BackendEntry> {
        self.entries.iter().find(|entry| entry.addr == addr)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::BackendSet;

    fn addrs() -> Vec<SocketAddr> {
        vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 2),
        ]
    }

    #[test]
    fn round_robin_among_healthy() -> Result<(), Box<dyn std::error::Error>> {
        let set = BackendSet::new(addrs());
        assert_eq!(set.select_next()?, addrs()[0]);
        assert_eq!(set.select_next()?, addrs()[1]);
        assert_eq!(set.select_next()?, addrs()[0]);
        Ok(())
    }

    #[test]
    fn skips_unhealthy_backend() -> Result<(), Box<dyn std::error::Error>> {
        let set = BackendSet::new(addrs());
        set.record_probe_failure(addrs()[0], 1);
        assert_eq!(set.select_next()?, addrs()[1]);
        assert_eq!(set.select_next()?, addrs()[1]);
        Ok(())
    }
}
