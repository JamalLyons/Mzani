//! Minimal HTTP response helpers for the proxy accept path.

use std::io::Write;
use std::net::TcpStream;

use crate::MzaniResult;

/// Writes a `503 Service Unavailable` response and shuts down the write half.
///
/// # Errors
///
/// Returns [`MzaniError`] on client write failures.
pub(crate) fn write_service_unavailable(stream: &mut TcpStream, message: &str) -> MzaniResult<()> {
    let body = format!("service unavailable: {message}");
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}

/// Writes a `413 Payload Too Large` response and shuts down the write half.
///
/// # Errors
///
/// Returns [`MzaniError`] on client write failures.
pub(crate) fn write_payload_too_large(stream: &mut TcpStream) -> MzaniResult<()> {
    const BODY: &str = "payload too large";
    let response = format!(
        "HTTP/1.1 413 Payload Too Large\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{BODY}",
        BODY.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    Ok(())
}
