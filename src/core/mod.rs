use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::MzaniResult;
use crate::core::pool::ThreadPool;
use crate::state::context::{Context, write_context};
use crate::utils::log_record::LogLevel;

pub(crate) mod pool;
pub(crate) mod request;
pub(crate) mod response;

const ACCEPT_READ_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECTION_QUEUE_CAPACITY: usize = 64;

/// Runs the load balancer accept loop and worker pool until the listener exits.
///
/// # Arguments
///
/// * `ctx` - Initialized load balancer context (listen address and backends)
///
/// # Errors
///
/// Returns [`MzaniError::Io`] if binding or accepting connections fails.
/// Returns [`MzaniError::LoggerInit`] if the log file cannot be created.
///
/// # Examples
///
/// ```no_run
/// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
///
/// use mzani::{Context, create_server};
///
/// let ctx = Context::new(vec![SocketAddr::from((
///     IpAddr::V4(Ipv4Addr::LOCALHOST),
///     8080,
/// ))])?;
/// create_server(ctx)?;
/// # Ok::<(), mzani::MzaniError>(())
/// ```
#[must_use = "the server runs until the listener is closed"]
pub fn create_server(ctx: Context) -> MzaniResult<()>
{
    let listen_addr = ctx.socket_addr();
    let shared = Arc::new(RwLock::new(ctx));
    let pool = ThreadPool::new(&shared)?;

    let listener = TcpListener::bind(listen_addr)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(error) = stream.set_read_timeout(Some(ACCEPT_READ_TIMEOUT)) {
                    if let Some(context) = write_context(&shared) {
                        context.log_accept_event(
                            "accept_read_timeout_set_failed",
                            LogLevel::Warn,
                            &[("error", error.to_string())],
                        );
                    }
                    continue;
                }

                if let Err(error) = pool.submit(stream)
                    && let Some(context) = write_context(&shared)
                {
                    context.log_accept_event(
                        "pool_saturated",
                        LogLevel::Warn,
                        &[
                            ("error", error.to_string()),
                            ("queue_capacity", CONNECTION_QUEUE_CAPACITY.to_string()),
                        ],
                    );
                }
            }
            Err(error) => {
                if let Some(context) = write_context(&shared) {
                    context.log_accept_event(
                        "accept_error",
                        LogLevel::Error,
                        &[("error", error.to_string()), ("listen", listen_addr.to_string())],
                    );
                }
            }
        }
    }

    Ok(())
}
