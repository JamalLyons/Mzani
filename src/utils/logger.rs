use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::utils::now;
use crate::{MzaniError, MzaniResult};

/// Thread-safe file logger for request and error output.
#[derive(Debug)]
pub(crate) struct Logger
{
    file_handle: Arc<Mutex<std::fs::File>>,
}

impl Logger
{
    /// Creates a logger that writes timestamped entries under `logs/`.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::LoggerInit`] if the log directory or file cannot be created.
    pub fn new() -> MzaniResult<Self>
    {
        let log_dir = PathBuf::from("logs");
        fs::create_dir_all(&log_dir)
            .map_err(|error| MzaniError::LoggerInit(format!("failed to create log directory: {error}")))?;

        let timestamp = now()?;
        let log_path = log_dir.join(format!("mzani-{timestamp}.log"));
        let file_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| MzaniError::LoggerInit(format!("failed to open log file {}: {error}", log_path.display())))?;

        Ok(Self {
            file_handle: Arc::new(Mutex::new(file_handle)),
        })
    }

    fn lock_file(&self) -> MzaniResult<MutexGuard<'_, std::fs::File>>
    {
        self.file_handle
            .lock()
            .map_err(|_| MzaniError::LoggerInit("log file mutex poisoned".to_owned()))
    }

    /// Writes a request preview line to the log file.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] if writing fails.
    pub fn log(&self, message: &str) -> MzaniResult<()>
    {
        let timestamp = now()?;
        let mut file = self.lock_file()?;
        writeln!(file, "[{timestamp}] REQUEST\n\n{message}\n").map_err(|error| MzaniError::io(&error))?;
        Ok(())
    }

    /// Writes an error or diagnostic line to the log file.
    ///
    /// # Errors
    ///
    /// Returns [`MzaniError::Io`] if writing fails.
    pub fn log_error(&self, message: &str) -> MzaniResult<()>
    {
        let timestamp = now()?;
        let mut file = self.lock_file()?;
        writeln!(file, "[{timestamp}] ERROR: {message}").map_err(|error| MzaniError::io(&error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests
{
    use std::fs;
    use std::io::Read;

    use super::Logger;

    #[test]
    fn log_writes_message() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_root = std::env::temp_dir().join(format!("mzani-log-test-{}", std::process::id()));
        fs::create_dir_all(&temp_root)?;
        let previous = std::env::current_dir()?;
        std::env::set_current_dir(&temp_root)?;

        let result = (|| {
            let logger = Logger::new()?;
            logger.log("hello proxy")?;
            let log_file = fs::read_dir("logs")?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| path.is_file())
                .ok_or("missing log file")?;
            let mut contents = String::new();
            std::fs::File::open(log_file)?.read_to_string(&mut contents)?;
            assert!(contents.contains("hello proxy"));
            Ok::<(), Box<dyn std::error::Error>>(())
        })();

        std::env::set_current_dir(previous)?;
        let _ = fs::remove_dir_all(temp_root);
        result
    }
}
