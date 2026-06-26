//! Leadership abstraction for high availability deployments.
//!
//! This module provides a trait-based abstraction for leader election,
//! allowing the exporter to run in active-passive mode with automatic failover.

#[cfg(feature = "kubernetes")]
pub mod kubernetes;
pub mod noop;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

/// Represents the current leadership state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipState {
    /// This instance is the leader and should collect metrics.
    Leader,
    /// This instance is on standby and should not collect metrics.
    #[allow(dead_code)] // Used by kubernetes feature and tests
    Standby,
}

impl LeadershipState {
    pub const fn is_leader(self) -> bool {
        matches!(self, Self::Leader)
    }
}

/// Shared leadership status that can be cheaply cloned and checked.
#[derive(Clone)]
pub struct LeadershipStatus {
    is_leader: Arc<AtomicBool>,
    #[allow(dead_code)] // Used by wait_for_change which is for future use
    state_rx: watch::Receiver<LeadershipState>,
}

impl LeadershipStatus {
    /// Create a new leadership status.
    pub fn new(initial_state: LeadershipState) -> (Self, LeadershipStateUpdater) {
        let is_leader = Arc::new(AtomicBool::new(initial_state.is_leader()));
        let (state_tx, state_rx) = watch::channel(initial_state);

        let status = Self {
            is_leader: Arc::clone(&is_leader),
            state_rx,
        };

        let updater = LeadershipStateUpdater {
            is_leader,
            state_tx,
        };

        (status, updater)
    }

    /// Check if this instance is currently the leader.
    /// This is a cheap atomic read, safe to call frequently.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Relaxed)
    }

    /// Get the current leadership state.
    #[allow(dead_code)] // For future use and tests
    pub fn state(&self) -> LeadershipState {
        *self.state_rx.borrow()
    }

    /// Wait for leadership state to change.
    #[allow(dead_code)] // For future use
    pub async fn wait_for_change(&mut self) -> LeadershipState {
        // Ignore error - sender is never dropped while we're running
        let _ = self.state_rx.changed().await;
        *self.state_rx.borrow()
    }
}

/// Used by leadership providers to update the leadership state.
#[allow(dead_code)] // Used by kubernetes feature and tests
pub struct LeadershipStateUpdater {
    is_leader: Arc<AtomicBool>,
    state_tx: watch::Sender<LeadershipState>,
}

impl LeadershipStateUpdater {
    /// Update the leadership state.
    #[allow(dead_code)] // Used by kubernetes feature and tests
    pub fn set_state(&self, state: LeadershipState) {
        self.is_leader.store(state.is_leader(), Ordering::Relaxed);
        // Ignore error - receiver might be dropped
        let _ = self.state_tx.send(state);
    }
}

/// Trait for leadership election providers.
#[async_trait::async_trait]
pub trait LeadershipProvider: Send + Sync {
    /// Start the leadership election process.
    /// Returns a handle that provides the current leadership status.
    async fn start(&self) -> crate::error::Result<LeadershipStatus>;

    /// Stop the leadership election process and release leadership.
    async fn stop(&self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leadership_state_is_leader() {
        assert!(LeadershipState::Leader.is_leader());
        assert!(!LeadershipState::Standby.is_leader());
    }

    #[tokio::test]
    async fn test_leadership_status_initial_state() {
        let (status, _updater) = LeadershipStatus::new(LeadershipState::Leader);
        assert!(status.is_leader());
        assert_eq!(status.state(), LeadershipState::Leader);
    }

    #[tokio::test]
    async fn test_leadership_status_update() {
        let (status, updater) = LeadershipStatus::new(LeadershipState::Standby);
        assert!(!status.is_leader());

        updater.set_state(LeadershipState::Leader);
        assert!(status.is_leader());
        assert_eq!(status.state(), LeadershipState::Leader);
    }

    #[tokio::test]
    async fn test_leadership_status_wait_for_change() {
        let (mut status, updater) = LeadershipStatus::new(LeadershipState::Standby);

        // Spawn a task to update state after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            updater.set_state(LeadershipState::Leader);
        });

        let new_state = status.wait_for_change().await;
        assert_eq!(new_state, LeadershipState::Leader);
    }
}
