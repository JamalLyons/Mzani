//! Isolated log directories for integration tests (std only, no tempfile crate).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use mzani::MzaniResult;

static WORKSPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Owns a temp directory used as the proxy log root for one integration test.
pub struct TestWorkspace {
    root: PathBuf,
    log_dir: PathBuf,
}

impl TestWorkspace {
    /// Creates a temp tree with a `logs/` subdirectory for mzani.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from directory creation.
    pub fn new() -> MzaniResult<Self> {
        let id = WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("mzani_it_{}_{id}", std::process::id()));
        let log_dir = root.join("logs");
        fs::create_dir_all(&log_dir).map_err(|error| mzani::MzaniError::Io(error.to_string()))?;
        Ok(Self { root, log_dir })
    }

    /// Log directory passed to [`mzani::Context::new_with_options`].
    #[must_use]
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }

    /// Path to the structured log file for assertions.
    #[must_use]
    pub fn log_file(&self) -> PathBuf {
        self.log_dir.join("mzani.log")
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
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
