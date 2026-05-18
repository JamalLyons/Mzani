# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-05-17

First production-oriented release: std-only HTTP/1.x reverse proxy with configurable limits,
background health probes, graceful shutdown, and a stable [`Balancer`](https://docs.rs/mzani/latest/mzani/struct.Balancer.html) API.

### Added

#### Public API

- [`Balancer`](https://docs.rs/mzani/latest/mzani/struct.Balancer.html) — `build`, `run`, `run_on_listener`, `shutdown`, `metrics_snapshot`, and access to the shared [`BackendSet`](https://docs.rs/mzani/latest/mzani/struct.BackendSet.html)
- [`BalancerConfig`](https://docs.rs/mzani/latest/mzani/struct.BalancerConfig.html) — listen address, backends, log directory, limits, timeouts, health settings, pool sizing, and `max_backend_retries`
- [`ShutdownReport`](https://docs.rs/mzani/latest/mzani/struct.ShutdownReport.html) — `joined_cleanly` and `connections_dropped` after graceful shutdown
- [`Limits`](https://docs.rs/mzani/latest/mzani/struct.Limits.html) — `max_header_bytes`, `max_body_bytes`, `max_response_bytes` (defaults: 64 KiB headers, 16 MiB bodies)
- [`Timeouts`](https://docs.rs/mzani/latest/mzani/struct.Timeouts.html) — client/backend read, accept-read, and slow-request warning thresholds
- [`HealthConfig`](https://docs.rs/mzani/latest/mzani/struct.HealthConfig.html) — probe interval, connect timeout, failure/recovery thresholds
- [`PoolConfig`](https://docs.rs/mzani/latest/mzani/struct.PoolConfig.html) — worker count, per-worker queue capacity, bounded log/metrics channel sizes
- [`BackendSet`](https://docs.rs/mzani/latest/mzani/struct.BackendSet.html) — thread-safe round-robin over backends with per-address health state
- [`RoutingStrategy`](https://docs.rs/mzani/latest/mzani/trait.RoutingStrategy.html) trait and [`RoundRobinRouting`](https://docs.rs/mzani/latest/mzani/struct.RoundRobinRouting.html) default implementation
- [`MetricsSnapshot`](https://docs.rs/mzani/latest/mzani/struct.MetricsSnapshot.html) and [`BackendMetrics`](https://docs.rs/mzani/latest/mzani/struct.BackendMetrics.html) — totals, pool rejections, dropped logs, average latency, per-backend request/error counts
- [`LogSink`](https://docs.rs/mzani/latest/mzani/trait.LogSink.html) trait for custom log consumers (file logger and channel sink implementations)
- [`RequestOutcome`](https://docs.rs/mzani/latest/mzani/enum.RequestOutcome.html) re-export for metrics consumers
- `Context::from_config`, `Context::new_with_listen`, and `Context::new_with_options` for embedding without `Balancer`
- `ThreadPool::shutdown_threads`, `log_sender`, `reject_pool_full`, and `metrics`/`dropped_logs` accessors on the worker pool

#### Runtime behavior

- Background **TCP health probes** with consecutive failure/recovery thresholds; structured log events `backend_unhealthy` and `backend_recovered` (`LogRole::Health`)
- **Graceful shutdown**: stop accept, drain workers, flush metrics snapshot, join threads in order (workers → detach observability → metrics → log)
- **Bounded `sync_channel`** queues for log and metrics records; `dropped_logs` counter and periodic `log_dropped` warnings
- **HTTP 503 Service Unavailable** when worker queues are full or no healthy backends are available (`write_service_unavailable`)
- **HTTP 413 Payload Too Large** when request or decoded response bodies exceed configured limits
- **Connect retries** to the next healthy backend on proxy connect failure (`max_backend_retries`, default 1)
- Configurable worker pool size and per-worker connection queue (defaults: 10 workers, queue capacity 7 per worker)
- `pool_rejected` metric when connections are turned away under load

#### Errors (`MzaniError`)

- `ShutdownInProgress` — pool will not accept work while shutting down
- `BodyTooLarge` — request/response exceeds `Limits`
- `NoHealthyBackend` — all backends marked unhealthy
- `ShutdownTimeout` — graceful shutdown deadline exceeded
- `InvalidConfig` — `BalancerConfig` validation failure

#### Binary (`mzani`)

- Environment variables `MZANI_LISTEN` (default `127.0.0.1:8080`) and `MZANI_BACKENDS` (comma-separated list, default `127.0.0.1:3333,127.0.0.1:3334`)

#### Examples and documentation

- [`examples/minimal_proxy.rs`](examples/minimal_proxy.rs) — minimal `Balancer::build` + `run` example
- [README.md](README.md) — quickstart, features, limitations (HTTP/1.x, no TLS), development commands
- Crate-level documentation in [`src/lib.rs`](src/lib.rs) with architecture overview
- [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE) (dual-licensed)
- [SECURITY.md](SECURITY.md), [CONTRIBUTING.md](CONTRIBUTING.md), and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

#### Tests

- [`tests/production.rs`](tests/production.rs) — health probes, graceful shutdown, burst traffic
- [`tests/stress.rs`](tests/stress.rs) — optional ignored stress burst (`cargo test --release -- --ignored stress_burst`)
- Expanded integration coverage in [`tests/traffic_director.rs`](tests/traffic_director.rs) (no `serial_test`; parallel-safe harness)
- In-tree test workspace helper ([`tests/support/workspace.rs`](tests/support/workspace.rs)) replacing `tempfile` dev-dependency
- Unit tests for body limits, backend health selection, pool observability, and parser garbage corpus

#### Tooling and CI

- [`.github/workflows/ci.yml`](.github/workflows/ci.yml) on Rust **1.85** (stable): `cargo fmt --check`, build, `clippy -D warnings`, `cargo test -- --test-threads=1`, `cargo doc`
- [`rust-toolchain.toml`](rust-toolchain.toml) pinned to 1.85 with rustfmt and clippy
- `rust-version = "1.85"` and crates.io metadata in [`Cargo.toml`](Cargo.toml) (description, license, repository, keywords, categories, `exclude`)

### Changed

#### Concurrency and performance

- **Removed global `RwLock::write` on the request hot path** — workers call `Context::handle_connection(&self, …)`; backend round-robin and health live in lock-free [`BackendSet`](src/state/backend.rs)
- `ThreadPool::submit` now returns `Result<(), (MzaniError, TcpStream)>` so callers can respond on the wire when the queue is full
- `Request::from_stream` accepts any `Read` source and enforces `Limits` on body size before allocation
- `read_http_response` enforces `max_response_bytes` for fixed, chunked, and EOF-delimited bodies
- Pool shutdown joins worker threads before detaching context observability senders, fixing a metrics-thread deadlock on drop

#### API ergonomics

- **`Context::new` no longer reads the listen address from process arguments** — use `BalancerConfig::listen`, `Context::new_with_listen`, or `Context::new_with_options`
- **`create_server`** builds a `Balancer` from an existing `Context` (legacy entry point; prefer `Balancer` directly)
- `handle_connection` takes `&self` instead of `&mut self` (embedders no longer need exclusive access for routing)
- Metrics reports include **pool rejected** counts and **per-backend** breakdowns
- Health monitor and balancer `Drop` stop background threads cleanly

#### Dependencies

- **Removed dev-dependencies** `serial_test` and `tempfile`; integration tests use in-tree helpers only (library remains std-only with zero runtime dependencies)

### Removed

- Silent drop of accepted TCP connections when the worker pool queue was full (clients now receive **503** when possible)
- Implicit listen-address discovery via `std::env::args()` in `Context::new`
- Nightly-only CI requirement (replaced with stable 1.85 MSRV workflow)

### Security

- Request and response bodies are capped by configurable limits to reduce memory exhaustion from large `Content-Length` or chunked payloads
- HTTP header blocks remain capped at 64 KiB by default (`Limits::max_header_bytes`)
- Pool saturation returns an explicit error response instead of leaving clients hanging without a status line

### Breaking changes (from pre-1.0 development)

If you were using earlier in-repo APIs before this release:

| Before | After |
|--------|--------|
| `Context::new(backends)` with argv listen address | `Balancer::build(BalancerConfig { … })` or `Context::new_with_listen(backends, listen)` |
| `ThreadPool::submit(stream) -> MzaniResult<()>` | `submit(stream) -> Result<(), (MzaniError, TcpStream)>` |
| `Arc<RwLock<Context>>` required for workers | `Arc<Context>` with interior channels; routing via `BackendSet` |
| No health or shutdown API | `Balancer::shutdown(timeout)` and background health probes |

[Unreleased]: https://github.com/JamalLyons/Mzani/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/JamalLyons/Mzani/releases/tag/v1.0.0
