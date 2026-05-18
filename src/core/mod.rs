use crate::MzaniResult;
use crate::balancer::Balancer;
use crate::config::BalancerConfig;
use crate::state::context::Context;

pub(crate) mod http_util;
pub(crate) mod pool;
pub(crate) mod request;
pub(crate) mod response;

/// Runs the load balancer accept loop until the listener exits.
///
/// Prefer [`Balancer::build`] and [`Balancer::run`] for new code.
///
/// # Errors
///
/// Returns [`MzaniError`] if configuration or startup fails.
///
/// # Examples
///
/// ```no_run
/// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
/// use std::sync::Arc;
///
/// use mzani::{BackendSet, BalancerConfig, Context, create_server};
///
/// let listen = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 8080));
/// let backends = vec![SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 9001))];
/// let config = BalancerConfig {
///     listen,
///     backends: backends.clone(),
///     ..BalancerConfig::default()
/// };
/// let backends_set = Arc::new(BackendSet::new(backends));
/// let ctx = Context::from_config(&config, &backends_set)?;
/// create_server(&ctx)?;
/// # Ok::<(), mzani::MzaniError>(())
/// ```
#[must_use = "the server runs until the listener is closed"]
pub fn create_server(ctx: &Context) -> MzaniResult<()> {
    let config = BalancerConfig {
        listen: ctx.socket_addr(),
        backends: ctx.target_servers(),
        log_dir: ctx.log_dir().to_path_buf(),
        limits: ctx.limits(),
        timeouts: ctx.timeouts(),
        ..BalancerConfig::default()
    };
    let mut balancer = Balancer::build(config)?;
    balancer.run()
}
