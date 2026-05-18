//! Production feature tests: health, shutdown, limits, pool saturation.

mod support;

use std::net::Ipv4Addr;
use std::thread;
use std::time::Duration;

use mzani::{Balancer, BalancerConfig, HealthConfig, MzaniResult};
use support::{BackendReply, HttpRequestSpec, MockBackend, ProxyHarness, TestWorkspace, exchange_via_proxy};

#[test]
fn health_marks_unreachable_backend_unhealthy() -> MzaniResult<()> {
    let workspace = TestWorkspace::new()?;
    let dead = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 59_999));

    let config = BalancerConfig {
        listen: std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 19_000)),
        backends: vec![dead],
        log_dir: workspace.log_dir().to_path_buf(),
        health: HealthConfig {
            interval: Duration::from_millis(100),
            connect_timeout: Duration::from_millis(50),
            failure_threshold: 1,
            recovery_threshold: 1,
        },
        ..BalancerConfig::default()
    };

    let balancer = Balancer::build(config)?;
    thread::sleep(Duration::from_millis(400));
    assert!(!balancer.backends().is_healthy(dead));
    Ok(())
}

#[test]
fn shutdown_joins_workers() -> MzaniResult<()> {
    let workspace = TestWorkspace::new()?;
    let dead = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 59_998));
    let config = BalancerConfig {
        listen: std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 19_001)),
        backends: vec![dead],
        log_dir: workspace.log_dir().to_path_buf(),
        health: HealthConfig {
            interval: Duration::from_secs(60),
            ..HealthConfig::default()
        },
        ..BalancerConfig::default()
    };
    let balancer = Balancer::build(config)?;
    let report = balancer.shutdown(Duration::from_secs(2))?;
    assert!(report.joined_cleanly);
    Ok(())
}

#[test]
fn burst_exchanges_on_shared_proxy() -> MzaniResult<()> {
    let workspace = TestWorkspace::new()?;
    let backend = MockBackend::spawn("b1", BackendReply::OkText("ok"), 10)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    for _ in 0..10 {
        let spec = HttpRequestSpec::get("/burst");
        let response = exchange_via_proxy(&mut proxy, &spec)?;
        assert_eq!(response.status, 200);
    }
    backend.join()
}
