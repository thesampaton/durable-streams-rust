#![forbid(unsafe_code)]

//! Scaffold for the Durable Streams Rust client crate.
//!
//! This crate intentionally contains only a minimal, documented placeholder so
//! the workspace, CI, and standards-alignment plumbing can be established
//! before the production client implementation is built.

/// Marker type for the current client scaffold phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientScaffold;

impl ClientScaffold {
    /// Returns the current implementation phase for the crate.
    #[must_use]
    pub const fn phase() -> &'static str {
        "scaffold"
    }
}

#[cfg(test)]
mod tests {
    use super::ClientScaffold;

    #[test]
    fn exposes_scaffold_phase() {
        assert_eq!(ClientScaffold::phase(), "scaffold");
    }
}
