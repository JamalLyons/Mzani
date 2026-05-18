use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::utils::log_record::{LogLevel, LogRecord};
use crate::{MzaniError, MzaniResult};

/// Thread-safe file logger for structured log records.
#[derive(Debug)]
pub(crate) struct Logger
{
    file_handle: Arc<Mutex<std::fs::File>>,
    min_level: LogLevel,
}

impl Logger
{
    /// Creates a logger that writes under `logs/` with level filter from `MZANI_LOG`.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::LoggerInit`] if the log directory or file cannot be created.
    pub fn new() -> MzaniResult<Self>
    {
        let log_dir = PathBuf::from("logs");
        fs::create_dir_all(&log_dir)
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

    fn lock_file(&self) -> MzaniResult<MutexGuard<'_, std::fs::File>>
    {
        self.file_handle
            .lock()
            .map_err(|_| MzaniError::LoggerInit("log file mutex poisoned".to_owned()))
    }

    /// Writes a structured log record as one line.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] if writing fails.
    pub fn write_record(&self, record: &LogRecord) -> MzaniResult<()>
    {
        if !record.meets_min_level(self.min_level) {
            return Ok(());
        }
        let line = record.format_line();
        let mut file = self.lock_file()?;
        writeln!(file, "{line}").map_err(|error| MzaniError::io(&error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests
{
    use std::fs;
    use std::io::Read;

    use super::Logger;
    use crate::utils::log_record::{LogLevel, LogRecord, LogRole};

    #[test]
    fn write_record_appends_line() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_root = std::env::temp_dir().join(format!("mzani-log-test-{}", std::process::id()));
        fs::create_dir_all(&temp_root)?;
        let previous = std::env::current_dir()?;
        std::env::set_current_dir(&temp_root)?;

        let result = (|| {
            let logger = Logger::new()?;
            let record = LogRecord::new(LogLevel::Info, "test_event", LogRole::Log).field("msg", "hello");
            logger.write_record(&record)?;
            let log_file = fs::read_dir("logs")?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| path.is_file())
                .ok_or("missing log file")?;
            let mut contents = String::new();
            fs::File::open(log_file)?.read_to_string(&mut contents)?;
            assert!(contents.contains("event=test_event"));
            assert!(contents.contains("msg=hello"));
            Ok::<(), Box<dyn std::error::Error>>(())
        })();

        std::env::set_current_dir(previous)?;
        let _ = fs::remove_dir_all(temp_root);
        result
    }
}
