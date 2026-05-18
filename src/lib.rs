//! # mzani
//!
//! A std-only HTTP load balancer library. Mzani (Swahili for "balance") accepts
//! client connections, parses HTTP/1.x requests, forwards them to backend servers
//! using round-robin selection, and records request metrics.

mod core;
mod state;
mod utils;

pub use crate::core::create_server;
pub use crate::core::pool::ThreadPool;
pub use crate::state::context::Context;
pub use crate::utils::error::{MzaniError, MzaniResult};
pub use crate::utils::log_record::RequestContext;
