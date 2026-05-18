# Mzani Core Development Todo List

## 📂 Project Structure
```text
mzani/
├── Cargo.toml
├── src/
│   ├── main.rs          # Entry point: CLI args & Thread Pool init
│   ├── lib.rs           # Module declarations
│   ├── core/
│   │   ├── mod.rs
│   │   ├── connection.rs # TCP handling & proxy logic
│   │   ├── request.rs     # Byte-to-Request struct logic
│   │   └── pool.rs       # Worker thread & Queue implementation
│   ├── state/
│   │   ├── mod.rs
│   │   ├── context.rs    # Shared state (Arc<RwLock>)
│   │   └── metrics.rs    # HashMap-based server tracking
│   └── utils/
│       ├── mod.rs
│       ├── logger.rs     # Multi-threaded file logging
│       └── errors.rs     # Custom MzaniResult & Error types
└── logs/                # Generated log files
```

## Type Definitions & Memory Safety
- [x] **Binary-First Buffer:** Replace `read_request_buffer_to_string` with a function that returns `Vec<u8>`.
- [x] **Custom Result Type:** Define `type MzaniResult<T> = Result<T, MzaniError>`.
- [x] **Error Handling:** Implement a custom `enum MzaniError` (wrap `std::io::Error`, add `ParseError`, `BackendTimeout`, etc.).
- [ ] **Zero-Panic Policy:** Replace all `.expect()` and `.unwrap()` in the connection path with proper error propagation (`?`).

## Request Parser (The Byte Engine)
- [x] **Request Struct:** Create a struct to hold:
    - `method`: String or Enum
    - `path`: String
    - `headers`: `Vec<(String, String)>`
    - `body`: `Vec<u8>`
- [x] **Header Parsing:** Implement a loop that reads from `TcpStream` into a buffer until it finds the byte pattern `\r\n\r\n`.
- [x] **Content-Length Logic:** Extract the length from headers and use `read_exact` to pull the remaining binary body.

## Concurrency (The Thread Pool)
- [ ] **State Sharing:** Wrap `Context` in `Arc<RwLock<Context>>` to allow multiple threads to read the server list while one thread manages metrics.
- [ ] **Job Queue:** Implement a `SyncSender/Receiver` (mpsc) channel to hand off `TcpStream` objects to workers.
- [ ] **Worker Loop:** Create a fixed number of threads (e.g., 10) that stay alive, waiting for streams from the channel.
- [ ] **Timeout Management:** Set `stream.set_read_timeout` to prevent slow-loris attacks or hung backends from blocking a worker forever.

## Metrics & Logging
- [ ] **Per-Server Metrics:** Change `Metrics` to a `HashMap<SocketAddr, ServerStats>`.
- [ ] **Aggregator:** Implement a method to summarize the map into the "Global Metrics" view you currently have.
- [ ] **Thread-Safe Logger:** Ensure the `Logger` can handle multiple threads writing to the same file (use `Arc<Mutex<File>>`).

## Reliability & Retries
- [ ] **Backend Health Check:** Add a "status" field to servers. If a connection fails, mark it "down" and skip it in the selection loop.
- [ ] **Retry Strategy:** If `handle_backend_stream` fails, the worker should automatically fetch the *next* server in the queue and try again (up to 3 times).
- [ ] **Dynamic Reconfiguration:** Add a "Control thread" that watches for a signal (or file change) to add/remove `SocketAddr` from the list without restarting the app.
