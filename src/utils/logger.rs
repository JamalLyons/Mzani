use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::utils::log_record::{LogLevel, LogRecord};
use crate::{MzaniError, MzaniResult};

/// Consumer for structured log records (file, channel, or test double).
pub trait LogSink: Send + Sync {
    /// Records one log event.
    fn emit(&self, record: LogRecord);
}

/// Thread-safe file logger for structured log records.
#[derive(Debug)]
pub(crate) struct Logger {
    file_handle: Arc<Mutex<std::fs::File>>,
    min_level: LogLevel,
}

impl Logger {
    /// Creates a logger that appends to `log_dir/mzani.log`.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::LoggerInit`] if the log directory or file cannot be created.
    pub fn new_in_dir(log_dir: &Path) -> MzaniResult<Self> {
        fs::create_dir_all(log_dir)
            .map_err(|error| MzaniError::LoggerInit(format!("failed to create log directory: {error}")))?;

        let log_path = log_dir.join("mzani.log");
        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| MzaniError::LoggerInit(format!("failed to open log file {}: {error}", log_path.display())))?;

        Ok(Self {
            file_handle: Arc::new(Mutex::new(file_handle)),
            min_level: LogLevel::from_env(),
        })
    }

    fn lock_file(&self) -> MzaniResult<MutexGuard<'_, std::fs::File>> {
        self.file_handle
            .lock()
            .map_err(|_| MzaniError::LoggerInit("log file mutex poisoned".to_owned()))
    }

    /// Writes a structured log record as one line.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] if writing fails.
    pub fn write_record(&self, record: &LogRecord) -> MzaniResult<()> {
        if !record.meets_min_level(self.min_level) {
            return Ok(());
        }
        let line = record.format_line();
        let mut file = self.lock_file()?;
        writeln!(file, "{line}").map_err(|error| MzaniError::io(&error))?;
        Ok(())
    }
}

impl LogSink for Logger {
    fn emit(&self, record: LogRecord) {
        let _ = self.write_record(&record);
    }
}

/// Forwards log records to an MPSC sender (drops on full queue).
#[derive(Debug)]
pub(crate) struct ChannelLogSink {
    tx: std::sync::mpsc::SyncSender<LogRecord>,
}

impl ChannelLogSink {
    /// Creates a sink that sends to `tx`, dropping when the channel is full.
    #[must_use]
    pub fn new(tx: std::sync::mpsc::SyncSender<LogRecord>) -> Self {
        Self { tx }
    }
}

impl LogSink for ChannelLogSink {
    fn emit(&self, record: LogRecord) {
        let _ = self.tx.try_send(record);
    }
}

impl LogSink for Arc<dyn LogSink> {
    fn emit(&self, record: LogRecord) {
        self.as_ref().emit(record);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Read;

    use super::Logger;
    use crate::utils::log_record::{LogLevel, LogRecord, LogRole};

    #[test]
    fn write_record_appends_line() -> Result<(), Box<dyn std::error::Error>> {
        let log_dir = std::env::temp_dir().join(format!("mzani_log_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&log_dir);
        fs::create_dir_all(&log_dir)?;
        let logger = Logger::new_in_dir(&log_dir)?;
        let record = LogRecord::new(LogLevel::Info, "test_event", LogRole::Log).field("msg", "hello");
        logger.write_record(&record)?;
        let mut contents = String::new();
        fs::File::open(log_dir.join("mzani.log"))?.read_to_string(&mut contents)?;
        assert!(contents.contains("event=test_event"));
        assert!(contents.contains("msg=hello"));
        let _ = fs::remove_dir_all(&log_dir);
        Ok(())
    }
}
