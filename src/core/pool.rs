use std::fmt;
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::state::context::{Context, write_context};
use crate::state::metrics::{Metrics, RequestStats};
use crate::utils::log_record::{LogLevel, LogRecord, LogRole, RequestContext, format_addr, next_request_id};
use crate::utils::logger::Logger;
use crate::{MzaniError, MzaniResult};

const DEFAULT_WORKER_COUNT: usize = 10;
const CONNECTION_QUEUE_CAPACITY: usize = 64;
const PER_WORKER_QUEUE_CAPACITY: usize = CONNECTION_QUEUE_CAPACITY.div_ceil(DEFAULT_WORKER_COUNT);
const METRICS_SNAPSHOT_EVERY_N: u64 = 100;
const METRICS_SNAPSHOT_INTERVAL: Duration = Duration::from_mins(1);

/// Role assigned to each thread in the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolRole
{
    ConnectionWorker,
    LogWriter,
    MetricsAggregator,
}

type PoolThreadHandle = (PoolRole, JoinHandle<()>);

/// Channels and shared state wired into [`Context`].
struct Observability
{
    log_tx: Option<Sender<LogRecord>>,
    metrics_tx: Option<Sender<RequestStats>>,
    metrics: Arc<Mutex<Metrics>>,
    next_req_id: Arc<AtomicU64>,
}

type ObservabilityStartup = (Observability, Vec<PoolThreadHandle>);

impl Observability
{
    fn start(context: &Arc<RwLock<Context>>) -> MzaniResult<ObservabilityStartup>
    {
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let next_req_id = Arc::new(AtomicU64::new(1));
        let dropped_logs = Arc::new(AtomicU64::new(0));
        let (log_tx, log_rx) = mpsc::channel();
        let (metrics_tx, metrics_rx) = mpsc::channel();

        let listen_addr;
        let backends;
        let log_dir;
        {
            let ctx = context
                .read()
                .map_err(|_| MzaniError::LoggerInit("context lock poisoned during pool start".to_owned()))?;
            listen_addr = ctx.socket_addr();
            log_dir = ctx.log_dir().to_path_buf();
            backends = ctx
                .target_servers()
                .iter()
                .map(|addr| format_addr(*addr))
                .collect::<Vec<_>>()
                .join(",");
        }

        let logger = Logger::new_in_dir(&log_dir)?;

        if let Some(mut ctx) = write_context(context) {
            ctx.attach_observability(
                log_tx.clone(),
                metrics_tx.clone(),
                Arc::clone(&metrics),
                Arc::clone(&next_req_id),
                Arc::clone(&dropped_logs),
            );
        }

        let startup_record = LogRecord::new(LogLevel::Info, "server_start", LogRole::Log)
            .field("listen", format_addr(listen_addr))
            .field("backends", backends)
            .field("worker_count", DEFAULT_WORKER_COUNT);

        let log_metrics_tx = log_tx.clone();
        let metrics_for_thread = Arc::clone(&metrics);
        let threads = vec![
            (PoolRole::LogWriter, thread::spawn(move || log_writer_loop(&log_rx, &logger))),
            (
                PoolRole::MetricsAggregator,
                thread::spawn(move || metrics_aggregator_loop(&metrics_rx, &metrics_for_thread, &log_metrics_tx)),
            ),
        ];

        let _ = log_tx.send(startup_record);

        Ok((
            Self {
                log_tx: Some(log_tx),
                metrics_tx: Some(metrics_tx),
                metrics,
                next_req_id,
            },
            threads,
        ))
    }

    fn detach_context(context: &Arc<RwLock<Context>>)
    {
        if let Some(mut ctx) = write_context(context) {
            ctx.detach_observability();
        }
    }

    fn shutdown_log(&mut self)
    {
        if let Some(tx) = self.log_tx.take()
            && let Ok(guard) = self.metrics.lock()
        {
            let _ = tx.send(guard.snapshot_record());
        }
    }
}

/// Worker pool with dedicated logging and metrics threads.
pub struct ThreadPool
{
    context: Arc<RwLock<Context>>,
    connection_txs: Vec<SyncSender<TcpStream>>,
    dispatch_index: AtomicUsize,
    observability: Observability,
    handles: Vec<PoolThreadHandle>,
}

impl fmt::Debug for ThreadPool
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
    {
        let connection_workers = self
            .handles
            .iter()
            .filter(|(role, _)| *role == PoolRole::ConnectionWorker)
            .count();
        f.debug_struct("ThreadPool")
            .field("connection_workers", &connection_workers)
            .finish_non_exhaustive()
    }
}

impl ThreadPool
{
    /// Spawns connection workers plus dedicated logging and metrics threads.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::LoggerInit`] if the log file cannot be created.
    pub fn new(context: &Arc<RwLock<Context>>) -> MzaniResult<Self>
    {
        let (observability, mut handles) = Observability::start(context)?;
        let mut connection_txs = Vec::with_capacity(DEFAULT_WORKER_COUNT);
        let next_req_id = Arc::clone(&observability.next_req_id);
        handles.reserve(DEFAULT_WORKER_COUNT);

        for worker_id in 0..DEFAULT_WORKER_COUNT {
            let (connection_tx, connection_rx) = mpsc::sync_channel(PER_WORKER_QUEUE_CAPACITY);
            connection_txs.push(connection_tx);
            let context = Arc::clone(context);
            let next_req_id = Arc::clone(&next_req_id);
            handles.push((
                PoolRole::ConnectionWorker,
                thread::spawn(move || connection_worker_loop(&connection_rx, &context, worker_id, &next_req_id)),
            ));
        }

        Ok(Self {
            context: Arc::clone(context),
            connection_txs,
            dispatch_index: AtomicUsize::new(0),
            observability,
            handles,
        })
    }

    /// Enqueues a client stream for a connection worker.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::PoolFull`] if the queue is saturated.
    pub fn submit(&self, stream: TcpStream) -> MzaniResult<()>
    {
        if self.connection_txs.is_empty() {
            return Err(MzaniError::PoolFull);
        }
        let worker_index = dispatch_worker_index(&self.dispatch_index, self.connection_txs.len());
        self.connection_txs[worker_index]
            .send(stream)
            .map_err(|_| MzaniError::PoolFull)
    }
}

impl Drop for ThreadPool
{
    fn drop(&mut self)
    {
        self.connection_txs.clear();
        self.observability.shutdown_log();
        Observability::detach_context(&self.context);
        self.observability.metrics_tx = None;

        for (_, handle) in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// Selects the next worker queue in round-robin order.
fn dispatch_worker_index(dispatch_index: &AtomicUsize, worker_count: usize) -> usize
{
    dispatch_index.fetch_add(1, Ordering::Relaxed) % worker_count
}

fn connection_worker_loop(
    receiver: &Receiver<TcpStream>,
    context: &Arc<RwLock<Context>>,
    worker_id: usize,
    next_req_id: &AtomicU64,
)
{
    while let Ok(stream) = receiver.recv() {
        let req_id = next_request_id(next_req_id);
        let req = RequestContext { req_id, worker_id };

        if let Some(mut context) = write_context(context) {
            let _ = context.handle_connection(stream, req);
        }
    }
}

fn log_writer_loop(receiver: &Receiver<LogRecord>, logger: &Logger)
{
    while let Ok(record) = receiver.recv() {
        let _ = logger.write_record(&record);
    }
}

fn metrics_aggregator_loop(receiver: &Receiver<RequestStats>, metrics: &Arc<Mutex<Metrics>>, log_tx: &Sender<LogRecord>)
{
    let mut request_count: u64 = 0;
    let mut last_snapshot = Instant::now();

    while let Ok(stats) = receiver.recv() {
        request_count += 1;
        let req = RequestContext {
            req_id: stats.req_id,
            worker_id: stats.worker_id,
        };

        let snapshot_due = match metrics.lock() {
            Ok(mut guard) => {
                guard.record_request(stats);
                request_count.is_multiple_of(METRICS_SNAPSHOT_EVERY_N)
                    || last_snapshot.elapsed() >= METRICS_SNAPSHOT_INTERVAL
            }
            Err(_) => continue,
        };

        let _ = log_tx.send(stats.to_metrics_record(req));

        if snapshot_due {
            last_snapshot = Instant::now();
            if let Ok(guard) = metrics.lock() {
                let _ = log_tx.send(guard.snapshot_record());
            }
        }
    }
}

#[cfg(test)]
mod tests
{
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, RwLock, mpsc};
    use std::time::Duration;

    use super::{DEFAULT_WORKER_COUNT, ThreadPool, dispatch_worker_index};
    use crate::state::context::Context;
    use crate::utils::log_record::{LogLevel, LogRecord, LogRole};

    fn test_servers() -> Vec<SocketAddr>
    {
        vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9_001)]
    }

    #[test]
    fn pool_attaches_observability_to_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = Arc::new(RwLock::new(Context::new(test_servers())?));
        let pool = ThreadPool::new(&context)?;

        {
            let ctx = context.read().map_err(|_| "lock poisoned")?;
            assert!(ctx.has_observability());
        }

        drop(pool);
        std::thread::sleep(Duration::from_millis(50));

        {
            let ctx = context.read().map_err(|_| "lock poisoned")?;
            assert!(!ctx.has_observability());
        }

        Ok(())
    }

    #[test]
    fn dispatch_worker_index_round_robins()
    {
        let dispatch = AtomicUsize::new(0);
        let mut seen = [0usize; DEFAULT_WORKER_COUNT];
        for _ in 0..DEFAULT_WORKER_COUNT * 3 {
            let worker = dispatch_worker_index(&dispatch, DEFAULT_WORKER_COUNT);
            seen[worker] += 1;
        }
        for count in seen {
            assert_eq!(count, 3);
        }
    }

    #[test]
    fn log_record_round_trip_via_channel() -> Result<(), Box<dyn std::error::Error>>
    {
        let (tx, rx) = mpsc::channel();
        let record = LogRecord::new(LogLevel::Error, "accept_error", LogRole::Accept).field("error", "test");
        tx.send(record)?;
        let received = rx.recv()?;
        assert_eq!(received.event, "accept_error");
        Ok(())
    }
}
