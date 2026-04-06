use crate::error::Error;
use crate::instrumentation as trace;
use crate::model::RetryOptions;
use std::future::Future;
use std::time::Duration;
use tracing::{Instrument, debug, warn};

/// Retry policy for transient operations.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    options: RetryOptions,
}

impl RetryPolicy {
    #[must_use]
    pub const fn new(options: RetryOptions) -> Self {
        Self { options }
    }

    pub fn validate(options: RetryOptions) -> Result<(), Error> {
        if options.initial_backoff.is_zero() {
            return Err(Error::invalid_argument(
                "retry initial_backoff must be greater than zero",
            ));
        }
        if options.max_backoff < options.initial_backoff {
            return Err(Error::invalid_argument(
                "retry max_backoff must be greater than or equal to initial_backoff",
            ));
        }
        if options.backoff_multiplier < 1.0 {
            return Err(Error::invalid_argument(
                "retry backoff_multiplier must be at least 1.0",
            ));
        }
        Ok(())
    }

    pub async fn run<T, F, Fut>(&self, mut operation: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut attempt = 0_u32;
        let mut delay = self.options.initial_backoff;

        loop {
            let attempt_number = attempt + 1;
            let span = trace::retry_span(attempt_number, self.options.max_retries);
            let result = operation().instrument(span.clone()).await;
            match result {
                Ok(value) => {
                    if attempt > 0 {
                        debug!(
                            parent: &span,
                            event = "retry.completed",
                            "retry.attempt" = attempt_number
                        );
                    }
                    return Ok(value);
                }
                Err(error) if attempt < self.options.max_retries && error.is_retryable() => {
                    trace::record_error(&span, &error);
                    span.record("retry.backoff_ms", delay.as_millis() as u64);
                    warn!(
                        parent: &span,
                        event = "retry.scheduled",
                        "retry.attempt" = attempt_number,
                        "retry.backoff_ms" = delay.as_millis() as u64
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    delay = next_delay(
                        delay,
                        self.options.max_backoff,
                        self.options.backoff_multiplier,
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[must_use]
pub fn next_delay(current: Duration, max_delay: Duration, multiplier: f64) -> Duration {
    let next = current.mul_f64(multiplier);
    std::cmp::min(next, max_delay)
}

#[cfg(test)]
mod tests {
    use super::next_delay;
    use std::time::Duration;

    #[test]
    fn caps_backoff_at_maximum() {
        let current = Duration::from_millis(100);
        let next = next_delay(current, Duration::from_millis(150), 2.0);
        assert_eq!(next, Duration::from_millis(150));
    }
}
