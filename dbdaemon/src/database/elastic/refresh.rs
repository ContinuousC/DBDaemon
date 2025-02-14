use std::time::{Duration, Instant};

use tokio::sync::watch;

#[derive(Debug)]
pub(crate) struct RefreshState {
    updated: Option<Instant>,
    refreshing: Option<watch::Receiver<()>>,
}

// Is there a better token available for this situation? The sender
// should simply drop the token. The receiver should wait until the
// watch channel returns an error.
pub(crate) enum RefreshResult {
    Fresh,
    Needed(watch::Sender<()>),
    Await(watch::Receiver<()>),
}

impl RefreshState {
    // Constructors.

    pub(crate) fn fresh() -> Self {
        Self {
            updated: None,
            refreshing: None,
        }
    }
    pub(crate) fn updated() -> Self {
        Self {
            updated: Some(Instant::now()),
            refreshing: None,
        }
    }

    pub(crate) fn refreshing() -> (Self, RefreshResult) {
        let (sender, receiver) = watch::channel(());
        (
            Self {
                updated: None,
                refreshing: Some(receiver),
            },
            RefreshResult::Needed(sender),
        )
    }

    // Update functions.

    /// Check if a refresh is needed or already in progress.
    pub(crate) fn needs_refresh(&mut self) -> RefreshResult {
        match &self.refreshing {
            Some(token) => RefreshResult::Await(token.clone()),
            None => match self.updated {
                Some(since)
                    if Instant::now().duration_since(since)
                        >= Duration::from_secs(60) =>
                {
                    let (state, result) = Self::refreshing();
                    *self = state;
                    result
                }
                _ => RefreshResult::Fresh,
            },
        }
    }

    /// Indicate that a refresh operation has been finished. The token
    /// is consumed here, thereby allowing any waiting operations to
    /// continue.
    pub(crate) fn refreshed(&mut self, _token: watch::Sender<()>) {
        self.refreshing = None;
    }

    /// Indicate that an update operation is about to be run.
    pub(crate) fn update(&mut self) {
        self.updated = Some(Instant::now());
    }
}
