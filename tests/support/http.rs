//! Minimal HTTP/1.1 framing helpers for integration tests (std-only).

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::TcpStream;

use mzani::MzaniResult;

const HEADER_END: &[u8] = b"\r\n\r\n";

/// Client request template used by the traffic-director tests.
#[derive(Debug, Clone)]
pub struct HttpRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpRequestSpec {
    /// GET with optional extra headers.
    #[must_use]
    pub fn get(path: &'static str) -> Self {
        Self {
            method: "GET",
            path,
            headers: vec![("Host".to_owned(), "mzani.test".to_owned())],
            body: Vec::new(),
        }
    }

    /// Request with a body; adds `Content-Length` automatically.
    #[must_use]
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// Appends a header pair.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Parsed HTTP response returned to the test client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Request observed by a mock backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Serializes an HTTP/1.1 request.
#[must_use]
pub fn build_request(spec: &HttpRequestSpec) -> Vec<u8> {
    let mut message = format!("{} {} HTTP/1.1\r\n", spec.method, spec.path);
    let mut headers = spec.headers.clone();
    if !spec.body.is_empty() && !headers.iter().any(|(name, _)| name.eq_ignore_ascii_case("content-length")) {
        headers.push(("Content-Length".to_owned(), String::new()));
    }

    for (name, value) in &headers {
        if name.eq_ignore_ascii_case("content-length") && value.is_empty() {
            let _ = write!(message, "Content-Length: {}\r\n", spec.body.len());
        } else {
            let _ = write!(message, "{name}: {value}\r\n");
        }
    }
    message.push_str("\r\n");
    let mut bytes = message.into_bytes();
    bytes.extend_from_slice(&spec.body);
    bytes
}

/// Sends a request to `addr` and reads the full response (until the server closes).
pub fn exchange(addr: std::net::SocketAddr, spec: &HttpRequestSpec) -> MzaniResult<HttpResponse> {
    use std::time::Duration;

    let mut stream = TcpStream::connect(addr)?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
    stream.write_all(&build_request(spec))?;
    stream.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    if raw.is_empty() {
        return Err(mzani::MzaniError::ParseError(
            "empty response from server (connection closed before headers)".to_owned(),
        ));
    }
    parse_response(&raw)
}

/// Parses status, headers, and body from a response buffer with `Content-Length` or chunked encoding.
pub fn parse_response(raw: &[u8]) -> MzaniResult<HttpResponse> {
    let header_end = raw
        .windows(HEADER_END.len())
        .position(|window| window == HEADER_END)
        .ok_or_else(|| mzani::MzaniError::ParseError("missing response header terminator".to_owned()))?;
    let head_len = header_end + HEADER_END.len();
    let head = std::str::from_utf8(&raw[..head_len])
        .map_err(|_| mzani::MzaniError::ParseError("invalid utf-8 in response".to_owned()))?;

    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| mzani::MzaniError::ParseError("missing status line".to_owned()))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| mzani::MzaniError::ParseError("missing status code".to_owned()))?
        .parse::<u16>()
        .map_err(|_| mzani::MzaniError::ParseError("invalid status code".to_owned()))?;

    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| mzani::MzaniError::ParseError("malformed response header".to_owned()))?;
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }

    let body = read_response_body(&headers, status, &raw[head_len..])?;
    Ok(HttpResponse { status, headers, body })
}

fn read_response_body(headers: &[(String, String)], status: u16, remainder: &[u8]) -> MzaniResult<Vec<u8>> {
    if matches!(status, 100..=199 | 204 | 304) {
        return Ok(Vec::new());
    }

    if headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked"))
    {
        return read_chunked_body(remainder);
    }

    if let Some((_, value)) = headers.iter().find(|(name, _)| name.eq_ignore_ascii_case("content-length")) {
        let length = value
            .parse::<usize>()
            .map_err(|_| mzani::MzaniError::ParseError("invalid content-length".to_owned()))?;
        if remainder.len() < length {
            return Err(mzani::MzaniError::ParseError("short response body".to_owned()));
        }
        return Ok(remainder[..length].to_vec());
    }

    Ok(remainder.to_vec())
}

fn read_chunked_body(mut input: &[u8]) -> MzaniResult<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line_end = input
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| mzani::MzaniError::ParseError("malformed chunk size line".to_owned()))?;
        let size_line = std::str::from_utf8(&input[..line_end])
            .map_err(|_| mzani::MzaniError::ParseError("invalid chunk size line".to_owned()))?;
        let size_token = size_line.split(';').next().unwrap_or(size_line).trim();
        let chunk_size = usize::from_str_radix(size_token, 16)
            .map_err(|_| mzani::MzaniError::ParseError("invalid chunk size".to_owned()))?;
        input = &input[line_end + 2..];

        if chunk_size == 0 {
            break;
        }

        if input.len() < chunk_size + 2 {
            return Err(mzani::MzaniError::ParseError("short chunked body".to_owned()));
        }
        body.extend_from_slice(&input[..chunk_size]);
        input = &input[chunk_size + 2..];
    }
    Ok(body)
}

/// Reads a complete HTTP/1.1 request from a backend connection.
pub fn read_request(stream: &mut TcpStream) -> MzaniResult<RecordedRequest> {
    let raw = read_message(stream)?;
    let header_end = raw
        .windows(HEADER_END.len())
        .position(|window| window == HEADER_END)
        .ok_or_else(|| mzani::MzaniError::ParseError("missing request header terminator".to_owned()))?;
    let head_len = header_end + HEADER_END.len();
    let head = std::str::from_utf8(&raw[..head_len])
        .map_err(|_| mzani::MzaniError::ParseError("invalid utf-8 in request".to_owned()))?;

    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| mzani::MzaniError::ParseError("missing request line".to_owned()))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| mzani::MzaniError::ParseError("missing method".to_owned()))?
        .to_owned();
    let path = parts
        .next()
        .ok_or_else(|| mzani::MzaniError::ParseError("missing path".to_owned()))?
        .to_owned();

    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| mzani::MzaniError::ParseError("malformed request header".to_owned()))?;
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }

    let body = read_request_body(&headers, &raw[head_len..])?;
    Ok(RecordedRequest {
        method,
        path,
        headers,
        body,
    })
}

fn read_request_body(headers: &[(String, String)], remainder: &[u8]) -> MzaniResult<Vec<u8>> {
    let length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| {
            value
                .parse::<usize>()
                .map_err(|_| mzani::MzaniError::ParseError("invalid content-length".to_owned()))
        })
        .transpose()?
        .unwrap_or(0);

    if remainder.len() < length {
        return Err(mzani::MzaniError::ParseError("short request body".to_owned()));
    }
    Ok(remainder[..length].to_vec())
}

fn read_message(stream: &mut TcpStream) -> MzaniResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            if buf.is_empty() {
                return Err(mzani::MzaniError::EmptyRequest);
            }
            break;
        }
        buf.extend_from_slice(&chunk[..read]);
        if buf.windows(HEADER_END.len()).any(|window| window == HEADER_END) {
            break;
        }
        if buf.len() > 128 * 1024 {
            return Err(mzani::MzaniError::ParseError("headers too large".to_owned()));
        }
    }

    let header_end = buf
        .windows(HEADER_END.len())
        .position(|window| window == HEADER_END)
        .ok_or_else(|| mzani::MzaniError::ParseError("missing header terminator".to_owned()))?
        + HEADER_END.len();
    let head =
        std::str::from_utf8(&buf[..header_end]).map_err(|_| mzani::MzaniError::ParseError("invalid utf-8".to_owned()))?;
    let content_length = head
        .lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .map_err(|_| mzani::MzaniError::ParseError("invalid content-length".to_owned()))?
        .unwrap_or(0);

    let body_in_buf = buf.len().saturating_sub(header_end);
    if body_in_buf < content_length {
        let mut rest = vec![0u8; content_length - body_in_buf];
        stream
            .read_exact(&mut rest)
            .map_err(|_| mzani::MzaniError::ParseError("short request body".to_owned()))?;
        buf.extend_from_slice(&rest);
    }

    Ok(buf)
}

/// Writes a raw HTTP response to the backend connection.
pub fn write_raw_response(stream: &mut TcpStream, raw: &[u8]) -> MzaniResult<()> {
    stream.write_all(raw)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{HttpRequestSpec, build_request, parse_response};

    #[test]
    fn build_request_adds_content_length_for_body() {
        let spec = HttpRequestSpec::get("/").with_body(b"payload".to_vec());
        let raw = build_request(&spec);
        let text = String::from_utf8_lossy(&raw);
        assert!(text.contains("Content-Length: 7"));
        assert!(text.ends_with("payload"));
    }

    #[test]
    fn parse_response_reads_content_length_body() -> Result<(), Box<dyn std::error::Error>> {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok";
        let parsed = parse_response(raw)?;
        assert_eq!(parsed.status, 201);
        assert_eq!(parsed.body, b"ok");
        Ok(())
    }
}
