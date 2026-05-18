//! End-to-end tests: mzani as an HTTP traffic director (parse, route, forward, return).

mod support;

use std::time::Duration;
use std::{fs, thread};

use mzani::MzaniResult;
use support::{
    BackendReply, HttpRequestSpec, MockBackend, ProxyHarness, TestWorkspace, exchange_via_proxy, with_temp_workspace,
};

const LARGE_BODY_BYTES: usize = 32 * 1024;

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[test]
fn forwards_each_standard_http_method() -> MzaniResult<()> {
    with_temp_workspace(forwards_each_standard_http_method_impl)
}

fn forwards_each_standard_http_method_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let methods = [
        ("GET", BackendReply::OkText("get-ok")),
        ("POST", BackendReply::OkText("post-ok")),
        ("PUT", BackendReply::OkText("put-ok")),
        ("DELETE", BackendReply::OkText("delete-ok")),
        ("HEAD", BackendReply::NoContent),
        ("OPTIONS", BackendReply::OkText("options-ok")),
        ("PATCH", BackendReply::OkText("patch-ok")),
    ];

    for (method, reply) in methods {
        let backend = MockBackend::spawn(method, reply, 1)?;
        let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

        let mut spec = HttpRequestSpec::get("/api/resource");
        spec.method = method;
        if method == "POST" || method == "PUT" || method == "PATCH" {
            spec = spec
                .with_body(b"{\"id\":1}".to_vec())
                .header("Content-Type", "application/json");
        }

        let response = exchange_via_proxy(&mut proxy, &spec)?;
        let recorded = backend.recordings()?;
        backend.join()?;
        assert_eq!(recorded.len(), 1, "method {method}");
        assert_eq!(recorded[0].method, method);
        assert_eq!(recorded[0].path, "/api/resource");

        if method == "HEAD" {
            assert_eq!(response.status, 204);
            assert!(response.body.is_empty());
        } else {
            assert_eq!(response.status, 200);
            assert!(!response.body.is_empty());
        }
    }

    Ok(())
}

#[test]
fn forwards_custom_headers_and_request_body() -> MzaniResult<()> {
    with_temp_workspace(forwards_custom_headers_and_request_body_impl)
}

fn forwards_custom_headers_and_request_body_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let body = br#"{"trace":"abc-123"}"#.to_vec();
    let backend = MockBackend::spawn("headers", BackendReply::OkText("ok"), 1)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let spec = HttpRequestSpec::get("/ingest")
        .with_body(body.clone())
        .header("Content-Type", "application/json")
        .header("X-Request-ID", "req-99")
        .header("X-Feature-Flag", "beta");

    let response = exchange_via_proxy(&mut proxy, &spec)?;
    let recorded = backend.recordings()?;
    backend.join()?;

    assert_eq!(recorded[0].body, body);
    assert_eq!(header_value(&recorded[0].headers, "x-request-id"), Some("req-99"));
    assert_eq!(header_value(&recorded[0].headers, "x-feature-flag"), Some("beta"));
    assert_eq!(header_value(&recorded[0].headers, "content-type"), Some("application/json"));
    assert_eq!(response.status, 200);
    Ok(())
}

#[test]
fn forwards_large_post_body_intact() -> MzaniResult<()> {
    with_temp_workspace(forwards_large_post_body_intact_impl)
}

fn forwards_large_post_body_intact_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let body = vec![b'Z'; LARGE_BODY_BYTES];
    let backend = MockBackend::spawn("large", BackendReply::OkText("big"), 1)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let spec = HttpRequestSpec::get("/upload")
        .header("Content-Type", "application/octet-stream")
        .with_body(body.clone());

    let mut spec = spec;
    spec.method = "POST";

    exchange_via_proxy(&mut proxy, &spec)?;
    let recorded = backend.recordings()?;
    backend.join()?;

    assert_eq!(recorded[0].body.len(), LARGE_BODY_BYTES);
    assert!(recorded[0].body.iter().all(|byte| *byte == b'Z'));
    Ok(())
}

#[test]
fn returns_chunked_backend_response_to_client() -> MzaniResult<()> {
    with_temp_workspace(returns_chunked_backend_response_to_client_impl)
}

fn returns_chunked_backend_response_to_client_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let backend = MockBackend::spawn("chunked", BackendReply::Chunked("chunked-ok"), 1)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let response = exchange_via_proxy(&mut proxy, &HttpRequestSpec::get("/chunked"))?;
    backend.join()?;

    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"chunked-ok");
    Ok(())
}

#[test]
fn returns_error_status_and_body_from_backend() -> MzaniResult<()> {
    with_temp_workspace(returns_error_status_and_body_from_backend_impl)
}

fn returns_error_status_and_body_from_backend_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let backend = MockBackend::spawn("404", BackendReply::NotFound("missing"), 1)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let response = exchange_via_proxy(&mut proxy, &HttpRequestSpec::get("/missing"))?;
    backend.join()?;

    assert_eq!(response.status, 404);
    assert_eq!(response.body, b"missing");
    Ok(())
}

#[test]
fn waits_for_slow_backend_before_responding() -> MzaniResult<()> {
    with_temp_workspace(waits_for_slow_backend_before_responding_impl)
}

fn waits_for_slow_backend_before_responding_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let delayed = b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\ndelayed";
    let backend = MockBackend::spawn(
        "slow",
        BackendReply::Delayed {
            wait: Duration::from_millis(120),
            inner: delayed,
        },
        1,
    )?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let started = std::time::Instant::now();
    let response = exchange_via_proxy(&mut proxy, &HttpRequestSpec::get("/slow"))?;
    backend.join()?;

    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"delayed");
    Ok(())
}

#[test]
fn round_robin_directs_traffic_across_backends() -> MzaniResult<()> {
    with_temp_workspace(round_robin_directs_traffic_across_backends_impl)
}

fn round_robin_directs_traffic_across_backends_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let backend_a = MockBackend::spawn("a", BackendReply::OkText("from-a"), 3)?;
    let backend_b = MockBackend::spawn("b", BackendReply::OkText("from-b"), 3)?;
    let mut proxy = ProxyHarness::start(vec![backend_a.addr, backend_b.addr], workspace.log_dir())?;

    for index in 0..6 {
        let seq = index.to_string();
        let spec = HttpRequestSpec::get("/rr").header("X-Seq", seq);
        exchange_via_proxy(&mut proxy, &spec)?;
    }

    let a_hits = backend_a.recordings()?;
    let b_hits = backend_b.recordings()?;
    backend_a.join()?;
    backend_b.join()?;
    assert_eq!(a_hits.len(), 3);
    assert_eq!(b_hits.len(), 3);

    let a_sequences = ["0", "2", "4"];
    let b_sequences = ["1", "3", "5"];
    for (record, expected_seq) in a_hits.iter().zip(a_sequences) {
        assert_eq!(header_value(&record.headers, "x-seq"), Some(expected_seq));
    }
    for (record, expected_seq) in b_hits.iter().zip(b_sequences) {
        assert_eq!(header_value(&record.headers, "x-seq"), Some(expected_seq));
    }

    Ok(())
}

#[test]
fn preserves_opaque_method_for_extension_verbs() -> MzaniResult<()> {
    with_temp_workspace(preserves_opaque_method_for_extension_verbs_impl)
}

fn preserves_opaque_method_for_extension_verbs_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let backend = MockBackend::spawn("custom", BackendReply::OkText("queued"), 1)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let mut spec = HttpRequestSpec::get("/jobs");
    spec.method = "PURGE";
    exchange_via_proxy(&mut proxy, &spec)?;
    let recorded = backend.recordings()?;
    backend.join()?;

    assert_eq!(recorded[0].method, "PURGE");
    Ok(())
}

#[test]
fn structured_logging_on_proxy_path() -> MzaniResult<()> {
    with_temp_workspace(run_logging_smoke_test)
}

fn run_logging_smoke_test(workspace: &TestWorkspace) -> MzaniResult<()> {
    let backend = MockBackend::spawn("log", BackendReply::OkText("ok"), 1)?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    exchange_via_proxy(&mut proxy, &HttpRequestSpec::get("/"))?;
    backend.join()?;

    thread::sleep(Duration::from_millis(50));

    let log_contents = fs::read_to_string(workspace.log_file())?;
    assert!(log_contents.contains("event=request_complete"));
    assert!(log_contents.contains("event=request_parsed"));
    assert!(!log_contents.contains("REQUEST\n\nGET"));
    Ok(())
}

#[test]
fn response_bytes_match_wire_format_for_status_line() -> MzaniResult<()> {
    with_temp_workspace(response_bytes_match_wire_format_for_status_line_impl)
}

fn response_bytes_match_wire_format_for_status_line_impl(workspace: &TestWorkspace) -> MzaniResult<()> {
    let backend = MockBackend::spawn(
        "created",
        BackendReply::Raw(b"HTTP/1.1 201 Created\r\nContent-Length: 7\r\n\r\ncreated"),
        1,
    )?;
    let mut proxy = ProxyHarness::start(vec![backend.addr], workspace.log_dir())?;

    let response = exchange_via_proxy(&mut proxy, &HttpRequestSpec::get("/created"))?;
    backend.join()?;

    assert_eq!(response.status, 201);
    assert_eq!(response.body, b"created");
    Ok(())
}
