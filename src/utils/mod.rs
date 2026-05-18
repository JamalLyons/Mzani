use std::time::{SystemTime, UNIX_EPOCH};

use crate::MzaniError;

pub(crate) mod error;
pub(crate) mod log_record;
pub(crate) mod logger;

/// Raw HTTP message bytes.
pub type Bytes = Vec<Byte>;

/// Single byte in HTTP messages.
pub type Byte = u8;

/// Returns whole seconds since the Unix epoch (legacy helper for file naming).
#[allow(dead_code)]
///
/// # Errors
///
/// Returns [`MzaniError::InvalidTimestamp`] if the system clock is before the epoch.
pub fn now() -> Result<u64, MzaniError>
{
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MzaniError::InvalidTimestamp)
        .map(|duration| duration.as_secs())
}

/// Builds a [`MzaniError::ParseError`] with the given message.
#[must_use]
pub fn parse_err(msg: &str) -> MzaniError
{
    MzaniError::ParseError(msg.to_owned())
}

#[cfg(test)]
mod tests
{
    use super::now;

    #[test]
    fn now_returns_positive_seconds() -> Result<(), crate::MzaniError>
    {
        let seconds = now()?;
        assert!(seconds > 0);
        Ok(())
    }
}
