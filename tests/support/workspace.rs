//! Isolated log directories for parallel integration tests.

use std::path::{Path, PathBuf};

use mzani::MzaniResult;
use tempfile::TempDir;

/// Owns a temp directory used as the proxy log root for one integration test.
pub struct TestWorkspace
{
    _temp: TempDir,
    log_dir: PathBuf,
}

impl TestWorkspace
{
    /// Creates a temp tree with a `logs/` subdirectory for mzani.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from [`TempDir::new`] or directory creation.
    pub fn new() -> MzaniResult<Self>
    {
        let temp = TempDir::new().map_err(|error| mzani::MzaniError::Io(error.to_string()))?;
        let log_dir = temp.path().join("logs");
        std::fs::create_dir_all(&log_dir).map_err(|error| mzani::MzaniError::Io(error.to_string()))?;
        Ok(Self { _temp: temp, log_dir })
    }

    /// Log directory passed to [`mzani::Context::new_with_options`].
    #[must_use]
    pub fn log_dir(&self) -> &Path
    {
        &self.log_dir
    }

    /// Path to the structured log file for assertions.
    #[must_use]
    pub fn log_file(&self) -> PathBuf
    {
        self.log_dir.join("mzani.log")
    }
}

/// Runs `test` with an isolated [`TestWorkspace`].
///
/// # Errors
///
/// Returns errors from workspace setup or the inner test.
pub fn with_temp_workspace<F>(test: F) -> MzaniResult<()>
where
    F: FnOnce(&TestWorkspace) -> MzaniResult<()>,
{
    let workspace = TestWorkspace::new()?;
    test(&workspace)
}
