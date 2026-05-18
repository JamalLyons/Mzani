use std::fmt;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::config::PoolConfig;
use crate::state::context::Context;
use crate::state::metrics::{Metrics, RequestStats};
use crate::utils::http::write_service_unavailable;
use crate::utils::log_record::{LogLevel, LogRecord, LogRole, RequestContext, format_addr, next_request_id};
use crate::utils::logger::Logger;
use crate::{MzaniError, MzaniResult};

const METRICS_SNAPSHOT_EVERY_N: u64 = 100;
const METRICS_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);

/// Role assigned to each thread in the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolRole {
    ConnectionWorker,
    LogWriter,
    MetricsAggregator,
}

type PoolThreadHandle = (PoolRole, JoinHandle<()>);

/// Channels and shared state wired into [`Context`].
struct Observability {
    log_tx: Option<SyncSender<LogRecord>>,
    metrics_tx: Option<SyncSender<RequestStats>>,
    metrics: Arc<Mutex<Metrics>>,
    dropped_logs: Arc<AtomicU64>,
}

type ObservabilityStartup = (Observability, Vec<PoolThreadHandle>);

impl Observability {
    fn start(context: &mut Context, pool_config: &PoolConfig) -> MzaniResult<ObservabilityStartup> {
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let dropped_logs = Arc::new(AtomicU64::new(0));
        let (log_tx, log_rx) = mpsc::sync_channel(pool_config.log_channel_capacity);
        let (metrics_tx, metrics_rx) = mpsc::sync_channel(pool_config.metrics_channel_capacity);

        let listen_addr = context.socket_addr();
        let log_dir = context.log_dir().to_path_buf();
        let backends = context
            .target_servers()
            .iter()
            .map(|addr| format_addr(*addr))
            .collect::<Vec<_>>()
            .join(",");

        let logger = Logger::new_in_dir(&log_dir)?;

        context.attach_observability(
            log_tx.clone(),
            metrics_tx.clone(),
            Arc::clone(&metrics),
            Arc::clone(&dropped_logs),
        );

        let startup_record = LogRecord::new(LogLevel::Info, "server_start", LogRole::Log)
            .field("listen", format_addr(listen_addr))
            .field("backends", backends)
            .field("worker_count", pool_config.workers);

        let log_metrics_tx = log_tx.clone();
        let metrics_for_thread = Arc::clone(&metrics);
        let dropped_for_thread = Arc::clone(&dropped_logs);
        let threads = vec![
            (PoolRole::LogWriter, thread::spawn(move || log_writer_loop(&log_rx, &logger))),
            (
                PoolRole::MetricsAggregator,
                thread::spawn(move || {
                    metrics_aggregator_loop(&metrics_rx, &metrics_for_thread, &log_metrics_tx, &dropped_for_thread);
                }),
            ),
        ];

        let _ = log_tx.try_send(startup_record);

        Ok((
            Self {
                log_tx: Some(log_tx),
                metrics_tx: Some(metrics_tx),
                metrics,
                dropped_logs,
            },
            threads,
        ))
    }

    fn shutdown_log(&mut self) {
        if let Some(tx) = self.log_tx.take() {
            if let Ok(guard) = self.metrics.lock() {
                let _ = tx.try_send(guard.snapshot_record());
            }
        }
    }
}

/// Worker pool with dedicated logging and metrics threads.
pub struct ThreadPool {
    context: Arc<Context>,
    connection_txs: Vec<SyncSender<TcpStream>>,
    dispatch_index: AtomicUsize,
    observability: Observability,
    handles: Vec<PoolThreadHandle>,
    shutting_down: Arc<AtomicBool>,
    pool_config: PoolConfig,
}

impl fmt::Debug for ThreadPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let connection_workers = self
            .handles
            .iter()
            .filter(|(role, _)| *role == PoolRole::ConnectionWorker)
            .count();
        f.debug_struct("ThreadPool")
            .field("connection_workers", &connection_workers)
            .field("per_worker_queue", &self.pool_config.per_worker_queue)
            .finish_non_exhaustive()
    }
}

impl ThreadPool {
    /// Spawns connection workers plus dedicated logging and metrics threads.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::LoggerInit`] if the log file cannot be created.
    pub fn new(mut context: Context, pool_config: &PoolConfig, shutting_down: Arc<AtomicBool>) -> MzaniResult<Self> {
        pool_config.validate()?;
        let (observability, mut handles) = Observability::start(&mut context, pool_config)?;
        let context = Arc::new(context);
        let next_req_id = Arc::new(AtomicU64::new(1));
        let mut connection_txs = Vec::with_capacity(pool_config.workers);
        handles.reserve(pool_config.workers);

        for worker_id in 0..pool_config.workers {
            let (connection_tx, connection_rx) = mpsc::sync_channel(pool_config.per_worker_queue);
            connection_txs.push(connection_tx);
            let ctx = Arc::clone(&context);
            let stop = Arc::clone(&shutting_down);
            let req_ids = Arc::clone(&next_req_id);
            handles.push((
                PoolRole::ConnectionWorker,
                thread::spawn(move || connection_worker_loop(&connection_rx, &ctx, worker_id, &stop, &req_ids)),
            ));
        }

        Ok(Self {
            context,
            connection_txs,
            dispatch_index: AtomicUsize::new(0),
            observability,
            handles,
            shutting_down,
            pool_config: *pool_config,
        })
    }

    /// Shared context for metrics snapshots.
    #[must_use]
    pub fn context(&self) -> &Arc<Context> {
        &self.context
    }

    /// Returns whether shutdown has been signaled.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }

    /// Enqueues a client stream for a connection worker.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::ShutdownInProgress`] or [`MzaniError::PoolFull`].
    pub fn submit(&self, stream: TcpStream) -> Result<(), (MzaniError, TcpStream)> {
        if self.shutting_down.load(Ordering::Relaxed) {
            return Err((MzaniError::ShutdownInProgress, stream));
        }
        if self.connection_txs.is_empty() {
            return Err((MzaniError::PoolFull, stream));
        }
        let worker_index = dispatch_worker_index(&self.dispatch_index, self.connection_txs.len());
        match self.connection_txs[worker_index].try_send(stream) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(stream)) => Err((MzaniError::PoolFull, stream)),
            Err(TrySendError::Disconnected(stream)) => Err((MzaniError::ShutdownInProgress, stream)),
        }
    }

    /// Clones the bounded log channel sender when observability is attached.
    #[must_use]
    pub fn log_sender(&self) -> Option<SyncSender<LogRecord>> {
        self.observability.log_tx.clone()
    }

    /// Rejects a connection with HTTP 503 and records pool saturation.
    pub fn reject_pool_full(&self, mut stream: TcpStream) {
        self.context.record_pool_rejected();
        let _ = write_service_unavailable(&mut stream, "worker queue full");
    }

    /// Drains worker queues by clearing senders; workers exit when channels close.
    pub fn stop_accepting(&mut self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        self.connection_txs.clear();
    }

    /// Metrics handle for snapshots.
    #[must_use]
    pub(crate) fn metrics(&self) -> &Arc<Mutex<Metrics>> {
        &self.observability.metrics
    }

    /// Dropped log counter.
    #[must_use]
    pub fn dropped_logs(&self) -> &Arc<AtomicU64> {
        &self.observability.dropped_logs
    }

    /// Joins all pool threads after stopping accept.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::LoggerInit`] if a worker thread panicked.
    pub fn shutdown_threads(&mut self) -> MzaniResult<()> {
        self.stop_accepting();

        let handles = std::mem::take(&mut self.handles);
        let mut workers = Vec::new();
        let mut metrics = None;
        let mut log = None;

        for (role, handle) in handles {
            match role {
                PoolRole::ConnectionWorker => workers.push(handle),
                PoolRole::MetricsAggregator => metrics = Some(handle),
                PoolRole::LogWriter => log = Some(handle),
            }
        }

        for handle in workers {
            if handle.join().is_err() {
                return Err(MzaniError::LoggerInit("worker thread panicked".to_owned()));
            }
        }

        if let Some(ctx) = Arc::get_mut(&mut self.context) {
            ctx.detach_observability();
        }

        self.observability.metrics_tx = None;

        if let Some(handle) = metrics {
            if handle.join().is_err() {
                return Err(MzaniError::LoggerInit("metrics thread panicked".to_owned()));
            }
        }

        self.observability.shutdown_log();
        self.observability.log_tx = None;

        if let Some(handle) = log {
            if handle.join().is_err() {
                return Err(MzaniError::LoggerInit("log thread panicked".to_owned()));
            }
        }

        Ok(())
    }
}

/// Selects the next worker queue in round-robin order.
fn dispatch_worker_index(dispatch_index: &AtomicUsize, worker_count: usize) -> usize {
    dispatch_index.fetch_add(1, Ordering::Relaxed) % worker_count
}

fn connection_worker_loop(
    receiver: &Receiver<TcpStream>,
    context: &Context,
    worker_id: usize,
    shutting_down: &AtomicBool,
    next_req_id: &AtomicU64,
) {
    while let Ok(mut stream) = receiver.recv() {
        if shutting_down.load(Ordering::Relaxed) {
            let _ = write_service_unavailable(&mut stream, "shutting down");
            continue;
        }
        let req_id = next_request_id(next_req_id);
        let req = RequestContext { req_id, worker_id };
        let _ = context.handle_connection(stream, req);
    }
}

fn log_writer_loop(receiver: &Receiver<LogRecord>, logger: &Logger) {
    while let Ok(record) = receiver.recv() {
        let _ = logger.write_record(&record);
    }
}

fn metrics_aggregator_loop(
    receiver: &Receiver<RequestStats>,
    metrics: &Arc<Mutex<Metrics>>,
    log_tx: &SyncSender<LogRecord>,
    dropped_logs: &AtomicU64,
) {
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
                (request_count % METRICS_SNAPSHOT_EVERY_N == 0) || last_snapshot.elapsed() >= METRICS_SNAPSHOT_INTERVAL
            }
            Err(_) => continue,
        };

        let _ = log_tx.try_send(stats.to_metrics_record(req));

        if snapshot_due {
            last_snapshot = Instant::now();
            if let Ok(guard) = metrics.lock() {
                let _ = log_tx.try_send(guard.snapshot_record());
                let _ = dropped_logs.load(Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::mpsc;

    use super::{PoolConfig, ThreadPool, dispatch_worker_index};
    use crate::config::BalancerConfig;
    use crate::state::context::Context;
    use crate::utils::log_record::{LogLevel, LogRecord, LogRole};

    fn test_listen() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9_000)
    }

    fn test_config() -> BalancerConfig {
        BalancerConfig {
            listen: test_listen(),
            backends: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9_001)],
            log_dir: std::env::temp_dir().join(format!("mzani_pool_test_{}", std::process::id())),
            pool: PoolConfig {
                workers: 2,
                per_worker_queue: 4,
                ..PoolConfig::default()
            },
            ..BalancerConfig::default()
        }
    }

    #[test]
    fn pool_attaches_observability_to_context() -> Result<(), Box<dyn std::error::Error>> {
        let config = test_config();
        let backends = Arc::new(crate::state::backend::BackendSet::new(config.backends.clone()));
        let context = Context::from_config(&config, &backends)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut pool = ThreadPool::new(context, &config.pool, stop)?;

        assert!(pool.log_sender().is_some());
        pool.shutdown_threads()?;
        Ok(())
    }

    #[test]
    fn dispatch_worker_index_round_robins() {
        let dispatch = AtomicUsize::new(0);
        let workers = 2;
        let mut seen = [0usize; 2];
        for _ in 0..workers * 3 {
            let worker = dispatch_worker_index(&dispatch, workers);
            seen[worker] += 1;
        }
        for count in seen {
            assert_eq!(count, 3);
        }
    }

    #[test]
    fn log_record_round_trip_via_sync_channel() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::sync_channel(4);
        let record = LogRecord::new(LogLevel::Error, "accept_error", LogRole::Accept).field("error", "test");
        tx.send(record)?;
        let received = rx.recv()?;
        assert_eq!(received.event, "accept_error");
        Ok(())
    }
}
