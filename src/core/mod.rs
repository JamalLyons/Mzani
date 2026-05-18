use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::MzaniResult;
use crate::core::pool::ThreadPool;
use crate::state::context::{Context, write_context};

pub(crate) mod pool;
pub(crate) mod request;

const ACCEPT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs the load balancer accept loop and worker pool until the listener exits.
///
/// # Arguments
///
/// * `ctx` - Initialized load balancer context (listen address and backends)
///
/// # Errors
///
/// Returns [`MzaniError::Io`] if binding or accepting connections fails.
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
    let pool = ThreadPool::new(&shared);

    let listener = TcpListener::bind(listen_addr)?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(error) = stream.set_read_timeout(Some(ACCEPT_READ_TIMEOUT)) {
                    if let Some(context) = write_context(&shared) {
                        context.log_error(&format!("failed to set read timeout: {error}"));
                    }
                    continue;
                }

                if let Err(error) = pool.submit(stream)
                    && let Some(context) = write_context(&shared)
                {
                    context.log_error(&format!("worker pool saturated: {error}"));
                }
            }
            Err(error) => {
                if let Some(context) = write_context(&shared) {
                    context.log_error(&format!("accept error: {error}"));
                }
            }
        }
    }

    Ok(())
}
