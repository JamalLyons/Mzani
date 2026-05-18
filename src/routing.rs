//! Pluggable backend selection strategies.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::MzaniResult;
use crate::state::backend::BackendSet;

/// Selects a backend for each proxied request.
pub trait RoutingStrategy: Send + Sync {
    /// Returns the next backend to use.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::NoHealthyBackend`] when no backend can serve traffic.
    fn select_backend(&self) -> MzaniResult<SocketAddr>;

    /// Returns all configured backend addresses (for logging and metrics).
    fn backend_addresses(&self) -> Vec<SocketAddr>;
}

/// Round-robin over healthy backends (default).
#[derive(Debug)]
pub struct RoundRobinRouting {
    backends: Arc<BackendSet>,
}

impl RoundRobinRouting {
    /// Creates a strategy backed by `backends`.
    #[must_use]
    pub fn new(backends: Arc<BackendSet>) -> Self {
        Self { backends }
    }
}

impl RoutingStrategy for RoundRobinRouting {
    fn select_backend(&self) -> MzaniResult<SocketAddr> {
        self.backends.select_next()
    }

    fn backend_addresses(&self) -> Vec<SocketAddr> {
        self.backends.addresses()
    }
}
