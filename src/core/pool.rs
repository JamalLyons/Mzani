use std::fmt;
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crate::state::context::{Context, write_context};
use crate::state::metrics::{Metrics, RequestStats};
use crate::utils::logger::Logger;
use crate::{MzaniError, MzaniResult};

const DEFAULT_WORKER_COUNT: usize = 10;
const CONNECTION_QUEUE_CAPACITY: usize = 64;

/// Role assigned to each thread in the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolRole
{
    /// Processes inbound TCP connections and proxies HTTP traffic.
    ConnectionWorker,
    /// Writes request previews, errors, and metrics reports to disk.
    LogWriter,
    /// Aggregates per-request statistics and triggers report logging.
    MetricsAggregator,
}

type PoolThreadHandle = (PoolRole, JoinHandle<()>);

/// Log event enqueued by connection workers and the metrics thread.
#[derive(Debug, Clone)]
pub(crate) enum LogMessage
{
    /// Request routing line or raw preview bytes (UTF-8 when possible).
    Request(String),
    /// Accept loop, pool, or connection failure.
    Error(String),
}

/// Channels and shared metrics state wired into [`Context`].
struct Observability
{
    log_tx: Option<Sender<LogMessage>>,
    metrics_tx: Option<Sender<RequestStats>>,
}

type ObservabilityStartup = (Observability, Vec<PoolThreadHandle>);

impl Observability
{
    fn start(context: &Arc<RwLock<Context>>) -> MzaniResult<ObservabilityStartup>
    {
        let logger = Logger::new()?;
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let (log_tx, log_rx) = mpsc::channel();
        let (metrics_tx, metrics_rx) = mpsc::channel();

        if let Some(mut ctx) = write_context(context) {
            ctx.attach_observability(log_tx.clone(), metrics_tx.clone(), Arc::clone(&metrics));
        }

        let log_metrics_tx = log_tx.clone();
        let metrics_for_thread = Arc::clone(&metrics);
        let threads = vec![
            (PoolRole::LogWriter, thread::spawn(move || log_writer_loop(&log_rx, &logger))),
            (
                PoolRole::MetricsAggregator,
                thread::spawn(move || metrics_aggregator_loop(&metrics_rx, &metrics_for_thread, &log_metrics_tx)),
            ),
        ];

        Ok((
            Self {
                log_tx: Some(log_tx),
                metrics_tx: Some(metrics_tx),
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
}

/// Worker pool with dedicated logging and metrics threads.
///
/// Constructed by [`crate::core::create_server`]; integration tests may use
/// [`ThreadPool::new`] to attach asynchronous observability before handling connections.
pub struct ThreadPool
{
    context: Arc<RwLock<Context>>,
    connection_tx: Option<SyncSender<TcpStream>>,
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
        let observability_threads = self
            .handles
            .iter()
            .filter(|(role, _)| *role != PoolRole::ConnectionWorker)
            .count();
        f.debug_struct("ThreadPool")
            .field("connection_workers", &connection_workers)
            .field("observability_threads", &observability_threads)
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
        let (connection_tx, connection_rx) = mpsc::sync_channel(CONNECTION_QUEUE_CAPACITY);
        let connection_rx = Arc::new(Mutex::new(connection_rx));
        handles.reserve(DEFAULT_WORKER_COUNT);

        for _ in 0..DEFAULT_WORKER_COUNT {
            let connection_rx = Arc::clone(&connection_rx);
            let context = Arc::clone(context);
            handles.push((
                PoolRole::ConnectionWorker,
                thread::spawn(move || connection_worker_loop(&connection_rx, &context)),
            ));
        }

        Ok(Self {
            context: Arc::clone(context),
            connection_tx: Some(connection_tx),
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
        let Some(sender) = &self.connection_tx else {
            return Err(MzaniError::PoolFull);
        };
        sender.send(stream).map_err(|_| MzaniError::PoolFull)
    }
}

impl Drop for ThreadPool
{
    fn drop(&mut self)
    {
        self.connection_tx = None;

        Observability::detach_context(&self.context);
        self.observability.log_tx = None;
        self.observability.metrics_tx = None;

        for (_, handle) in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn connection_worker_loop(receiver: &Arc<Mutex<Receiver<TcpStream>>>, context: &Arc<RwLock<Context>>)
{
    loop {
        let stream = {
            let Ok(receiver) = receiver.lock() else {
                break;
            };
            match receiver.recv() {
                Ok(stream) => stream,
                Err(_) => break,
            }
        };

        if let Some(mut context) = write_context(context)
            && let Err(error) = context.handle_connection(stream)
        {
            context.log_error(&format!("connection error: {error}"));
        }
    }
}

fn log_writer_loop(receiver: &Receiver<LogMessage>, logger: &Logger)
{
    while let Ok(message) = receiver.recv() {
        match message {
            LogMessage::Request(text) => {
                let _ = logger.log(&text);
            }
            LogMessage::Error(text) => {
                let _ = logger.log_error(&text);
            }
        }
    }
}

fn metrics_aggregator_loop(receiver: &Receiver<RequestStats>, metrics: &Arc<Mutex<Metrics>>, log_tx: &Sender<LogMessage>)
{
    while let Ok(stats) = receiver.recv() {
        let report = match metrics.lock() {
            Ok(mut guard) => {
                guard.record_request(stats);
                guard.format_report()
            }
            Err(_) => continue,
        };
        let _ = log_tx.send(LogMessage::Request(report));
    }
}

#[cfg(test)]
mod tests
{
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use super::{LogMessage, ThreadPool};
    use crate::state::context::Context;

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
    fn log_message_round_trip_via_channel() -> Result<(), Box<dyn std::error::Error>>
    {
        let (tx, rx) = std::sync::mpsc::channel();
        tx.send(LogMessage::Error("test".to_owned()))?;
        let msg = rx.recv()?;
        assert!(matches!(msg, LogMessage::Error(ref s) if s == "test"));
        Ok(())
    }
}
