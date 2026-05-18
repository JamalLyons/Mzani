//! Integration tests for end-to-end HTTP proxying.
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use mzani::{Context, MzaniError, MzaniResult};

#[test]
fn proxies_request_to_backend() -> MzaniResult<()>
{
    let backend_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let backend_addr = backend_listener.local_addr()?;

    let backend_thread = thread::spawn(move || -> MzaniResult<()> {
        let (mut stream, _) = backend_listener.accept()?;
        let mut buffer = [0u8; 1024];
        let _ = stream.read(&mut buffer)?;
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")?;
        stream.flush()?;
        Ok(())
    });

    let inbound_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let inbound_addr = inbound_listener.local_addr()?;

    let proxy_thread = thread::spawn(move || -> MzaniResult<()> {
        let (inbound, _) = inbound_listener.accept()?;
        let mut context = Context::new(vec![backend_addr])?;
        context.handle_connection(inbound)
    });

    let mut client = TcpStream::connect(inbound_addr)?;
    client.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    client.flush()?;

    let mut response = Vec::new();
    client.read_to_end(&mut response)?;

    proxy_thread
        .join()
        .map_err(|_| MzaniError::ParseError("proxy thread panicked".to_owned()))??;

    let body = String::from_utf8_lossy(&response);
    assert!(body.contains("ok"));

    thread::sleep(Duration::from_millis(20));
    let _ = backend_thread.join();
    Ok(())
}
