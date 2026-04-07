use super::Subscription;
use crate::error::Error;
use crate::model::SubscriptionEvent;

impl Subscription {
    /// Receive the next subscription event, or `None` when the task has finished.
    pub async fn next(&mut self) -> Option<Result<SubscriptionEvent, Error>> {
        self.receiver.recv().await
    }

    /// Abort the background subscription task.
    pub fn abort(&self) {
        self.task.abort();
    }
}
