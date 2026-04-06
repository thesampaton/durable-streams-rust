#![forbid(unsafe_code)]

//! Placeholder crate for the Durable Streams Rust server.
//!
//! The production server code lives elsewhere today. This crate exists so the
//! workspace has an explicit home for a future migration without forcing that
//! migration as part of the initial repository scaffold.

/// Marker type for the future server workspace member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerPlaceholder;

impl ServerPlaceholder {
    /// Returns the current implementation phase for the crate.
    #[must_use]
    pub const fn phase() -> &'static str {
        "placeholder"
    }
}

#[cfg(test)]
mod tests {
    use super::ServerPlaceholder;

    #[test]
    fn exposes_placeholder_phase() {
        assert_eq!(ServerPlaceholder::phase(), "placeholder");
    }
}
