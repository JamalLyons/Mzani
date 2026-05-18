//! Binary entry point for the mzani load balancer.
#![allow(clippy::print_stdout)]

use std::env;
use std::net::SocketAddr;

use mzani::{Balancer, BalancerConfig, MzaniResult};

fn main() -> MzaniResult<()> {
    let listen = listen_addr_from_env()?;
    let backends = backends_from_env()?;

    let config = BalancerConfig {
        listen,
        backends,
        ..BalancerConfig::default()
    };

    let mut balancer = Balancer::build(config)?;
    println!("listening on {}", balancer.listen_addr());
    balancer.run()
}

fn listen_addr_from_env() -> MzaniResult<SocketAddr> {
    let default = "127.0.0.1:8080";
    let addr_str = env::var("MZANI_LISTEN").unwrap_or_else(|_| default.to_owned());
    addr_str
        .parse()
        .map_err(|error| mzani::MzaniError::ParseError(format!("invalid MZANI_LISTEN: {error}")))
}

fn backends_from_env() -> MzaniResult<Vec<SocketAddr>> {
    let default = "127.0.0.1:3333,127.0.0.1:3334";
    let raw = env::var("MZANI_BACKENDS").unwrap_or_else(|_| default.to_owned());
    let mut addrs = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        addrs.push(
            trimmed
                .parse()
                .map_err(|error| mzani::MzaniError::ParseError(format!("invalid backend {trimmed}: {error}")))?,
        );
    }
    if addrs.is_empty() {
        return Err(mzani::MzaniError::EmptyServerList);
    }
    Ok(addrs)
}
