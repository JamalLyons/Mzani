use std::{error, fmt, io};

/// Result type alias for mzani operations.
pub type MzaniResult<T> = Result<T, MzaniError>;

/// Domain errors for the mzani load balancer.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MzaniError {
    /// Wrapped I/O error.
    Io(String),
    /// Request or configuration parse failure.
    ParseError(String),
    /// Operation exceeded the configured timeout.
    Timeout,
    /// No bytes were received for a request.
    EmptyRequest,
    /// Backend target list was empty.
    EmptyServerList,
    /// Logger could not be initialized.
    LoggerInit(String),
    /// System clock returned a time before the Unix epoch.
    InvalidTimestamp,
    /// Worker connection queue is full.
    PoolFull,
    /// Pool is shutting down and cannot accept work.
    ShutdownInProgress,
    /// Request or response body exceeds configured limits.
    BodyTooLarge,
    /// No healthy backends are available for routing.
    NoHealthyBackend,
    /// Graceful shutdown exceeded the deadline.
    ShutdownTimeout,
    /// Configuration validation failed.
    InvalidConfig(String),
}

impl MzaniError {
    /// Wrap an I/O error into [`MzaniError::Io`].
    #[must_use]
    pub fn io(error: &io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl fmt::Display for MzaniError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(f, "IO error: {message}"),
            Self::ParseError(message) => write!(f, "Parse error: {message}"),
            Self::Timeout => write!(f, "Timeout"),
            Self::EmptyRequest => write!(f, "Empty request"),
            Self::EmptyServerList => write!(f, "Target server list cannot be empty"),
            Self::LoggerInit(message) => write!(f, "Logger init error: {message}"),
            Self::InvalidTimestamp => write!(f, "System time is before Unix epoch"),
            Self::PoolFull => write!(f, "Worker pool queue is full"),
            Self::ShutdownInProgress => write!(f, "Load balancer is shutting down"),
            Self::BodyTooLarge => write!(f, "HTTP message exceeds size limit"),
            Self::NoHealthyBackend => write!(f, "No healthy backends available"),
            Self::ShutdownTimeout => write!(f, "Shutdown deadline exceeded"),
            Self::InvalidConfig(message) => write!(f, "Invalid configuration: {message}"),
        }
    }
}

impl error::Error for MzaniError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl From<io::Error> for MzaniError {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::TimedOut {
            Self::Timeout
        } else {
            Self::io(&error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MzaniError;

    #[test]
    fn display_parse_error() {
        let err = MzaniError::ParseError("bad header".to_owned());
        assert_eq!(err.to_string(), "Parse error: bad header");
    }

    #[test]
    fn display_empty_server_list() {
        let err = MzaniError::EmptyServerList;
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn display_no_healthy_backend() {
        let err = MzaniError::NoHealthyBackend;
        assert!(err.to_string().contains("healthy"));
    }
}
