use std::net::TcpStream;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crate::state::context::{Context, write_context};
use crate::{MzaniError, MzaniResult};

const DEFAULT_WORKER_COUNT: usize = 10;
const CHANNEL_CAPACITY: usize = 64;

/// Fixed-size worker pool that processes inbound TCP connections.
pub(crate) struct ThreadPool
{
    sender: std::sync::mpsc::SyncSender<TcpStream>,
    handles: Vec<JoinHandle<()>>,
}

impl ThreadPool
{
    /// Spawns workers that read connections from a shared queue.
    pub fn new(context: &Arc<RwLock<Context>>) -> Self
    {
        let (sender, receiver) = std::sync::mpsc::sync_channel(CHANNEL_CAPACITY);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut handles = Vec::with_capacity(DEFAULT_WORKER_COUNT);

        for _ in 0..DEFAULT_WORKER_COUNT {
            let receiver = Arc::clone(&receiver);
            let context = Arc::clone(context);
            handles.push(thread::spawn(move || worker_loop(&receiver, &context)));
        }

        Self { sender, handles }
    }

    /// Enqueues a client stream for a worker thread.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::PoolFull`] if the queue is saturated.
    pub fn submit(&self, stream: TcpStream) -> MzaniResult<()>
    {
        self.sender.send(stream).map_err(|_| MzaniError::PoolFull)
    }
}

impl Drop for ThreadPool
{
    fn drop(&mut self)
    {
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn worker_loop(receiver: &Arc<Mutex<std::sync::mpsc::Receiver<TcpStream>>>, context: &Arc<RwLock<Context>>)
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
