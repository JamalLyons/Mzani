//! Minimal mzani reverse proxy example.
#![allow(clippy::print_stdout)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use mzani::{Balancer, BalancerConfig};

fn main() -> Result<(), mzani::MzaniError> {
    let config = BalancerConfig {
        listen: SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)),
        backends: vec![SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 9001))],
        ..BalancerConfig::default()
    };

    let mut balancer = Balancer::build(config)?;
    println!("mzani listening on {}", balancer.listen_addr());
    balancer.run()
}
