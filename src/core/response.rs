use std::io::{self, Read};

use crate::MzaniError;
use crate::MzaniResult;
use crate::config::Limits;
use crate::core::request::{header_field, parse_header_fields, read_header_block};
use crate::utils::{Byte, Bytes, parse_err};

const CHUNK_HEADER_END: &[Byte] = b"\r\n";

enum BodyMode {
    Fixed(usize),
    Chunked,
    UntilEof,
}

/// Reads a complete HTTP/1.x response from `stream` (headers + body).
///
/// Uses `Content-Length` or chunked framing when present so the caller does not
/// need the backend to close the connection (required for keep-alive backends
/// and async handlers that respond after a delay).
///
/// # Errors
///
/// Returns [`MzaniError`] on I/O failures, malformed headers, or framing errors.
pub(crate) fn read_http_response(stream: &mut impl Read, limits: &Limits) -> MzaniResult<Bytes> {
    let (raw_head, body_prefix) = read_header_block(stream, limits.max_header_bytes)?;
    let headers = parse_header_fields(&raw_head)?;
    let status = parse_status_code(&raw_head)?;
    let mode = body_mode(&headers, status)?;

    let mut reader = PrefixReader::new(body_prefix, stream);
    let body = match mode {
        BodyMode::Fixed(len) => read_fixed_body(&mut reader, len, limits.max_response_bytes)?,
        BodyMode::Chunked => read_chunked_body(&mut reader, limits.max_response_bytes)?,
        BodyMode::UntilEof => read_until_eof(&mut reader, limits.max_response_bytes)?,
    };

    encode_forwarded_response(&raw_head, status, &headers, &body)
}

/// Rebuilds a response with an explicit `Content-Length` for transparent proxying.
fn encode_forwarded_response(raw_head: &[u8], status: u16, headers: &[(String, String)], body: &[u8]) -> MzaniResult<Bytes> {
    if matches!(status, 100..=199 | 204 | 304) {
        return Ok(raw_head.to_vec());
    }

    let head = std::str::from_utf8(raw_head).map_err(|_| parse_err("invalid utf-8 in response"))?;
    let status_line = head.lines().next().ok_or_else(|| parse_err("missing status line"))?;

    let mut lines = vec![status_line.to_owned()];
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("transfer-encoding") || name.eq_ignore_ascii_case("content-length") {
            continue;
        }
        lines.push(format!("{name}: {value}"));
    }
    lines.push(format!("Content-Length: {}", body.len()));

    let mut message = lines.join("\r\n").into_bytes();
    message.extend_from_slice(b"\r\n\r\n");
    message.extend_from_slice(body);
    Ok(message)
}

fn parse_status_code(raw_head: &[u8]) -> MzaniResult<u16> {
    let head = std::str::from_utf8(raw_head).map_err(|_| parse_err("invalid utf-8 in status line"))?;
    let status_line = head.lines().next().ok_or_else(|| parse_err("missing status line"))?;
    let mut parts = status_line.split_whitespace();
    let _version = parts.next().ok_or_else(|| parse_err("missing http version"))?;
    let code = parts
        .next()
        .ok_or_else(|| parse_err("missing status code"))?
        .parse::<u16>()
        .map_err(|_| parse_err("invalid status code"))?;
    Ok(code)
}

fn body_mode(headers: &[(String, String)], status: u16) -> MzaniResult<BodyMode> {
    if matches!(status, 100..=199 | 204 | 304) {
        return Ok(BodyMode::Fixed(0));
    }

    if header_field(headers, "transfer-encoding").is_some_and(|value| value.eq_ignore_ascii_case("chunked")) {
        return Ok(BodyMode::Chunked);
    }

    if let Some(value) = header_field(headers, "content-length") {
        let length = value
            .parse::<usize>()
            .map_err(|_| parse_err("invalid content-length in response"))?;
        return Ok(BodyMode::Fixed(length));
    }

    Ok(BodyMode::UntilEof)
}

struct PrefixReader<'a, R> {
    prefix: Bytes,
    offset: usize,
    inner: &'a mut R,
}

impl<'a, R> PrefixReader<'a, R>
where
    R: Read,
{
    fn new(prefix: Bytes, inner: &'a mut R) -> Self {
        Self {
            prefix,
            offset: 0,
            inner,
        }
    }
}

impl<R> Read for PrefixReader<'_, R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.offset < self.prefix.len() {
            let available = self.prefix.len() - self.offset;
            let n = available.min(buf.len());
            buf[..n].copy_from_slice(&self.prefix[self.offset..self.offset + n]);
            self.offset += n;
            return Ok(n);
        }
        self.inner.read(buf)
    }
}

fn read_fixed_body(stream: &mut impl Read, length: usize, max_bytes: usize) -> MzaniResult<Bytes> {
    if length > max_bytes {
        return Err(MzaniError::BodyTooLarge);
    }
    let mut body = Vec::with_capacity(length);
    let mut scratch = [0u8; 4096];
    while body.len() < length {
        let need = length - body.len();
        let take = need.min(scratch.len());
        let n = stream.read(&mut scratch[..take])?;
        if n == 0 {
            return Err(parse_err("unexpected end of response body"));
        }
        body.extend_from_slice(&scratch[..n]);
        if body.len() > max_bytes {
            return Err(MzaniError::BodyTooLarge);
        }
    }
    if body.len() > length {
        return Err(parse_err("response body exceeds content-length"));
    }
    Ok(body)
}

fn read_until_eof(stream: &mut impl Read, max_bytes: usize) -> MzaniResult<Bytes> {
    let mut body = Vec::new();
    let mut scratch = [0u8; 4096];
    loop {
        let n = stream.read(&mut scratch)?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&scratch[..n]);
        if body.len() > max_bytes {
            return Err(MzaniError::BodyTooLarge);
        }
    }
    Ok(body)
}

fn read_chunked_body(stream: &mut impl Read, max_bytes: usize) -> MzaniResult<Bytes> {
    let mut body = Vec::new();
    loop {
        let size_line = read_line(stream)?;
        let size_line = std::str::from_utf8(&size_line).map_err(|_| parse_err("invalid chunk size line"))?;
        let size_token = size_line.split(';').next().unwrap_or(size_line).trim();
        let chunk_size = usize::from_str_radix(size_token, 16).map_err(|_| parse_err("invalid chunk size"))?;

        if chunk_size == 0 {
            let _ = read_line(stream)?;
            break;
        }

        if body.len().saturating_add(chunk_size) > max_bytes {
            return Err(MzaniError::BodyTooLarge);
        }

        let mut chunk = vec![0u8; chunk_size];
        stream.read_exact(&mut chunk)?;
        body.extend_from_slice(&chunk);
        read_exact_delimiter(stream, CHUNK_HEADER_END)?;
    }

    Ok(body)
}

fn read_line(stream: &mut impl Read) -> MzaniResult<Bytes> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);
            return Ok(line);
        }
        if line.len() > 8 * 1024 {
            return Err(parse_err("chunk metadata line too long"));
        }
    }
}

fn read_exact_delimiter(stream: &mut impl Read, delimiter: &[u8]) -> MzaniResult<()> {
    let mut buf = vec![0u8; delimiter.len()];
    stream.read_exact(&mut buf)?;
    if buf != delimiter {
        return Err(parse_err("malformed chunked body"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::read_http_response;

    #[test]
    fn reads_response_with_content_length() -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        let mut cursor = Cursor::new(payload.to_vec());
        let limits = crate::config::Limits::default();
        let message = read_http_response(&mut cursor, &limits)?;
        assert_eq!(message, payload.as_slice());
        Ok(())
    }

    #[test]
    fn reads_response_without_waiting_for_connection_close() -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
        let mut cursor = Cursor::new(payload.to_vec());
        let limits = crate::config::Limits::default();
        let message = read_http_response(&mut cursor, &limits)?;
        assert!(message.ends_with(b"ok"));
        assert_eq!(cursor.position(), payload.len() as u64);
        Ok(())
    }

    #[test]
    fn reads_chunked_response() -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
        let mut cursor = Cursor::new(payload.to_vec());
        let limits = crate::config::Limits::default();
        let message = read_http_response(&mut cursor, &limits)?;
        let text = std::str::from_utf8(&message)?;
        assert!(text.contains("Content-Length: 5"));
        assert!(!text.contains("Transfer-Encoding"));
        assert!(message.ends_with(b"hello"));
        Ok(())
    }

    #[test]
    fn no_body_for_204() -> Result<(), Box<dyn std::error::Error>> {
        let payload = b"HTTP/1.1 204 No Content\r\n\r\n";
        let mut cursor = Cursor::new(payload.to_vec());
        let limits = crate::config::Limits::default();
        let message = read_http_response(&mut cursor, &limits)?;
        assert_eq!(message, payload.as_slice());
        Ok(())
    }
}
