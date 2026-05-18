//! Binary entry point for the mzani load balancer.
#![allow(clippy::print_stdout)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use mzani::{Context, MzaniResult, create_server};

fn main() -> MzaniResult<()>
{
    let ctx = Context::new(vec![
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3333),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3334),
    ])?;

    println!("listening on {}", ctx.socket_addr());

    create_server(ctx)?;

    Ok(())
}
