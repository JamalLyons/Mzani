use std::borrow::Cow;
use std::io::Read;
use std::net::TcpStream;

use crate::utils::{Byte, Bytes, parse_err};
use crate::{MzaniError, MzaniResult};

const HEADER_END: &[Byte] = b"\r\n\r\n";
const MAX_HEADER_SIZE: usize = 64 * 1024;

type ParsedHead = (HttpMethod, String, Vec<(String, String)>);

/// HTTP request method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HttpMethod
{
    Get,
    Post,
    Put,
    Delete,
    Head,
    Options,
    Patch,
    Other(Cow<'static, str>),
}

impl HttpMethod
{
    fn parse(s: &str) -> Self
    {
        match s {
            "GET" => Self::Get,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "DELETE" => Self::Delete,
            "HEAD" => Self::Head,
            "OPTIONS" => Self::Options,
            "PATCH" => Self::Patch,
            other => Self::Other(Cow::Owned(other.to_owned())),
        }
    }

    /// Returns the method as an HTTP token string.
    #[must_use]
    pub fn as_str(&self) -> &str
    {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Patch => "PATCH",
            Self::Other(value) => value.as_ref(),
        }
    }
}

/// Parsed HTTP/1.x request.
#[derive(Debug, Clone)]
pub(crate) struct Request
{
    method: HttpMethod,
    path: String,
    headers: Vec<(String, String)>,
    body: Bytes,
    raw_head: Bytes,
}

impl Request
{
    /// Reads and parses an HTTP request from a TCP stream.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError`] on I/O failures, malformed headers, or empty input.
    pub fn from_stream(stream: &mut TcpStream) -> MzaniResult<Self>
    {
        let (raw_head, body_prefix) = read_header_block(stream)?;
        let (method, path, headers) = parse_head(&raw_head)?;
        let content_length = content_length(&headers)?;

        let mut body = body_prefix;
        let remaining = content_length.saturating_sub(body.len());
        if remaining > 0 {
            let mut rest = vec![0u8; remaining];
            stream.read_exact(&mut rest)?;
            body.extend_from_slice(&rest);
        }

        Ok(Self {
            method,
            path,
            headers,
            body,
            raw_head,
        })
    }

    /// Returns the request method.
    #[must_use]
    pub fn method(&self) -> &HttpMethod
    {
        &self.method
    }

    /// Returns the request path.
    #[must_use]
    pub fn path(&self) -> &str
    {
        &self.path
    }

    /// Returns the request headers.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)]
    {
        &self.headers
    }

    /// Returns the request body bytes.
    #[must_use]
    pub fn body(&self) -> &[u8]
    {
        &self.body
    }

    /// Serializes the request back to wire-format bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes
    {
        let mut out = Vec::with_capacity(self.raw_head.len() + self.body.len());
        out.extend_from_slice(&self.raw_head);
        out.extend_from_slice(&self.body);
        out
    }
}

/// Reads bytes until the HTTP header terminator (`\r\n\r\n`).
pub(crate) fn read_header_block(stream: &mut impl Read) -> MzaniResult<(Bytes, Bytes)>
{
    const CHUNK_SIZE: usize = 4096;
    let mut buf: Bytes = Vec::with_capacity(CHUNK_SIZE);
    let mut chunk = [0u8; CHUNK_SIZE];

    loop {
        if buf.len() > MAX_HEADER_SIZE {
            return Err(parse_err("header block exceeds maximum size"));
        }

        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(MzaniError::EmptyRequest);
        }
        buf.extend_from_slice(&chunk[..n]);

        if let Some(end) = find_subslice(&buf, HEADER_END) {
            let head_len = end + HEADER_END.len();
            let body_prefix = buf[head_len..].to_vec();
            let raw_head = buf[..head_len].to_vec();
            return Ok((raw_head, body_prefix));
        }
    }
}

/// Parses header fields from an HTTP message head (request or response).
pub(crate) fn parse_header_fields(raw_head: &[u8]) -> MzaniResult<Vec<(String, String)>>
{
    let head = std::str::from_utf8(raw_head).map_err(|_| parse_err("invalid utf-8 in headers"))?;
    let mut lines = head.split("\r\n");
    let _start_line = lines.next().ok_or_else(|| parse_err("missing start line"))?;

    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| parse_err("malformed header line"))?;
        headers.push((name.trim().to_owned(), value.trim().to_owned()));
    }

    Ok(headers)
}

fn parse_head(raw_head: &[u8]) -> MzaniResult<ParsedHead>
{
    let head = std::str::from_utf8(raw_head).map_err(|_| parse_err("invalid utf-8 in headers"))?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or_else(|| parse_err("missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = HttpMethod::parse(parts.next().ok_or_else(|| parse_err("missing method"))?);
    let path = parts.next().ok_or_else(|| parse_err("missing path"))?.to_owned();
    let headers = parse_header_fields(raw_head)?;

    Ok((method, path, headers))
}

/// Returns the value of the first header matching `name` (case-insensitive).
pub(crate) fn header_field<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str>
{
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn content_length(headers: &[(String, String)]) -> MzaniResult<usize>
{
    match header_field(headers, "content-length") {
        Some(value) => value.parse::<usize>().map_err(|_| parse_err("invalid content-length")),
        None => Ok(0),
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize>
{
    haystack.windows(needle.len()).position(|window| window == needle)
}

#[cfg(test)]
mod tests
{
    use std::io::Cursor;

    use super::{HEADER_END, HttpMethod, Request, content_length, find_subslice, parse_head, read_header_block};

    #[test]
    fn find_subslice_locates_delimiter()
    {
        let haystack = b"GET / HTTP/1.1\r\n\r\n";
        let index = find_subslice(haystack, HEADER_END);
        let Some(index) = index else {
            panic!("expected header delimiter");
        };
        assert_eq!(&haystack[index..index + HEADER_END.len()], HEADER_END);
    }

    #[test]
    fn parse_head_extracts_method_and_path() -> Result<(), Box<dyn std::error::Error>>
    {
        let raw = b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let (method, path, headers) = parse_head(raw)?;
        assert_eq!(method, HttpMethod::Get);
        assert_eq!(path, "/health");
        assert_eq!(headers.len(), 1);
        Ok(())
    }

    #[test]
    fn content_length_defaults_to_zero() -> Result<(), Box<dyn std::error::Error>>
    {
        let headers = vec![("Host".to_owned(), "localhost".to_owned())];
        assert_eq!(content_length(&headers)?, 0);
        Ok(())
    }

    #[test]
    fn read_headers_reads_until_block_end() -> Result<(), Box<dyn std::error::Error>>
    {
        let payload = b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\nbody";
        let mut cursor = Cursor::new(payload.to_vec());
        let (raw_head, body_prefix) = read_header_block(&mut cursor)?;
        assert!(raw_head.ends_with(HEADER_END));
        assert_eq!(body_prefix, b"body");
        Ok(())
    }

    #[test]
    fn parse_head_rejects_invalid_utf8()
    {
        let raw = [0xff, 0xfe, 0xfd];
        assert!(parse_head(&raw).is_err());
    }

    #[test]
    fn accessors_expose_parsed_parts() -> Result<(), Box<dyn std::error::Error>>
    {
        let raw = b"POST /submit HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n";
        let (method, path, headers) = parse_head(raw)?;
        let request = Request {
            method,
            path,
            headers,
            body: Vec::new(),
            raw_head: raw.to_vec(),
        };
        assert_eq!(request.method().as_str(), "POST");
        assert_eq!(request.path(), "/submit");
        assert_eq!(request.headers().len(), 2);
        assert!(request.body().is_empty());
        Ok(())
    }
}
