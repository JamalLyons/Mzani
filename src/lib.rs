//! # mzani
//!
//! A std-only HTTP/1.x load balancer library. Mzani (Swahili for "balance") accepts
//! client connections on a listen address, parses HTTP/1.x requests, forwards them to
//! backend servers using round-robin over healthy nodes, and records structured metrics.
//!
//! ## Architecture
//!
//! - **Accept thread** — binds TCP and enqueues connections to worker queues.
//! - **Worker pool** — fixed threads proxy requests without holding a global write lock.
//! - **Health monitor** — background TCP probes mark backends up/down.
//! - **Log and metrics threads** — bounded channels aggregate observability off the hot path.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//!
//! use mzani::{Balancer, BalancerConfig};
//!
//! let config = BalancerConfig {
//!     listen: SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)),
//!     backends: vec![SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 9001))],
//!     ..BalancerConfig::default()
//! };
//! let mut balancer = Balancer::build(config)?;
//! balancer.run()?;
//! # Ok::<(), mzani::MzaniError>(())
//! ```
//!
//! For graceful shutdown, use [`Balancer::shutdown`] with a timeout after stopping accept
//! (for example by closing the listener from another thread).

mod balancer;
mod config;
mod core;
mod routing;
mod state;
mod utils;

pub use crate::balancer::{Balancer, ShutdownReport};
pub use crate::config::{BalancerConfig, HealthConfig, Limits, PoolConfig, Timeouts};
pub use crate::core::create_server;
pub use crate::core::pool::ThreadPool;
pub use crate::routing::{RoundRobinRouting, RoutingStrategy};
pub use crate::state::backend::BackendSet;
pub use crate::state::context::Context;
pub use crate::state::metrics::{BackendMetrics, MetricsSnapshot, RequestOutcome};
pub use crate::utils::error::{MzaniError, MzaniResult};
pub use crate::utils::log_record::RequestContext;
pub use crate::utils::logger::LogSink;
