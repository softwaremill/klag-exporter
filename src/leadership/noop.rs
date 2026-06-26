//! No-op leadership provider for single-instance deployments.
//!
//! This provider always reports as leader, providing backward compatibility
//! for deployments that don't need high availability.

use super::{LeadershipProvider, LeadershipState, LeadershipStatus};
use crate::error::Result;

/// A no-op leadership provider that always reports as leader.
///
/// Use this for single-instance deployments where leader election is not needed.
pub struct NoopLeader;

impl NoopLeader {
    pub const fn new() -> Self {
        Self
    }
}

impl Default for NoopLeader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LeadershipProvider for NoopLeader {
    async fn start(&self) -> Result<LeadershipStatus> {
        // Always leader in single-instance mode
        let (status, _updater) = LeadershipStatus::new(LeadershipState::Leader);
        // Note: updater is dropped but that's fine - state never changes
        Ok(status)
    }

    async fn stop(&self) {
        // Nothing to clean up
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_leader_always_leader() {
        let provider = NoopLeader::new();
        let status = provider.start().await.unwrap();

        assert!(status.is_leader());
        assert_eq!(status.state(), LeadershipState::Leader);
    }

    #[tokio::test]
    async fn test_noop_leader_default() {
        let status = NoopLeader.start().await.unwrap();
        assert!(status.is_leader());
    }
}
