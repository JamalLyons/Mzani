# mzani

Mzani (Swahili for "balance") is a **stdlib-only** HTTP/1.x load balancer library for Rust. It accepts TCP connections, parses HTTP/1.x requests, forwards them to backends using round-robin over **healthy** nodes, and records structured metrics and logs.

## Features

- Fixed worker thread pool with bounded per-worker queues and **503** responses when saturated
- Background **TCP health probes** with unhealthy backend skipping
- **Graceful shutdown** with a configurable drain timeout
- Configurable **request/response size limits**
- Structured logging and in-process metrics snapshots
- Pluggable [`RoutingStrategy`](https://docs.rs/mzani/latest/mzani/trait.RoutingStrategy.html) (default: round-robin)

## Requirements

- Rust **1.85+** (edition 2024)
- No external runtime dependencies in the library crate

## Quick start

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use mzani::{Balancer, BalancerConfig};

fn main() -> Result<(), mzani::MzaniError> {
    let config = BalancerConfig {
        listen: SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)),
        backends: vec![SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 9001))],
        ..BalancerConfig::default()
    };
    let mut balancer = Balancer::build(config)?;
    balancer.run()
}
```

Or run the example binary:

```bash
MZANI_LISTEN=127.0.0.1:8080 MZANI_BACKENDS=127.0.0.1:9001,127.0.0.1:9002 cargo run --example minimal_proxy
```

## Configuration

See [`BalancerConfig`](https://docs.rs/mzani/latest/mzani/struct.BalancerConfig.html) for listen address, backends, [`Limits`](https://docs.rs/mzani/latest/mzani/struct.Limits.html), [`Timeouts`](https://docs.rs/mzani/latest/mzani/struct.Timeouts.html), [`HealthConfig`](https://docs.rs/mzani/latest/mzani/struct.HealthConfig.html), and [`PoolConfig`](https://docs.rs/mzani/latest/mzani/struct.PoolConfig.html).

## Graceful shutdown

```rust
use std::time::Duration;

let balancer = mzani::Balancer::build(config)?;
// ... run accept loop on another thread, then:
let report = balancer.shutdown(Duration::from_secs(30))?;
assert!(report.joined_cleanly);
```

## Limitations

- HTTP/1.x only (no TLS termination in-tree)
- Round-robin / custom routing only (no HTTP/2)
- Blocking I/O on worker threads (by design)

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
```

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
