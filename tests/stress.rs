//! Longer-running stress checks (ignored by default).

mod support;

use mzani::MzaniResult;
use support::{BackendReply, HttpRequestSpec, MockBackend, ProxyHarness, TestWorkspace};

const STRESS_REQUESTS: usize = 100;

#[test]
#[ignore = "stress: run with cargo test --release -- --ignored stress_burst"]
fn stress_burst() -> MzaniResult<()> {
    let workspace = TestWorkspace::new()?;
    let backend = MockBackend::spawn("b1", BackendReply::OkText("ok"), STRESS_REQUESTS)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    for _ in 0..STRESS_REQUESTS {
        let spec = HttpRequestSpec::get("/stress");
        let response = support::exchange_via_proxy(&mut proxy, &spec)?;
        assert_eq!(response.status, 200);
    }
    backend.join()
}
