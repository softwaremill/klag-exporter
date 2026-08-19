use std::future::Future;
use std::time::Duration;

const BASE_BACKOFF_MS: u64 = 50;
const MAX_BACKOFF_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryStopReason {
    NonRetriable,
    Exhausted,
}

#[derive(Debug)]
pub(crate) struct RetryFailure<E> {
    pub error: E,
    pub attempts: usize,
    pub reason: RetryStopReason,
}

/// Execute an asynchronous operation once, then retry retriable failures up to
/// `max_retries` additional times.
pub(crate) async fn retry_with_backoff<T, E, Operation, OperationFuture, Retriable, Backoff>(
    max_retries: usize,
    mut operation: Operation,
    is_retriable: Retriable,
    mut backoff: Backoff,
) -> std::result::Result<(T, usize), RetryFailure<E>>
where
    Operation: FnMut(usize) -> OperationFuture,
    OperationFuture: Future<Output = std::result::Result<T, E>>,
    Retriable: Fn(&E) -> bool,
    Backoff: FnMut(usize) -> Duration,
{
    let max_attempts = max_retries.saturating_add(1);

    for attempt in 1..=max_attempts {
        match operation(attempt).await {
            Ok(value) => return Ok((value, attempt)),
            Err(error) => {
                if !is_retriable(&error) {
                    return Err(RetryFailure {
                        error,
                        attempts: attempt,
                        reason: RetryStopReason::NonRetriable,
                    });
                }
                if attempt == max_attempts {
                    return Err(RetryFailure {
                        error,
                        attempts: attempt,
                        reason: RetryStopReason::Exhausted,
                    });
                }

                let retry_number = attempt;
                let delay = backoff(retry_number);
                tokio::time::sleep(delay).await;
            }
        }
    }

    unreachable!("the retry loop always returns on its final attempt")
}

/// Full-jitter exponential backoff: retry 1 waits 0-50 ms, retry 2 waits
/// 0-100 ms, and subsequent caps double up to 1 second.
pub(crate) fn full_jitter_backoff(retry_number: usize) -> Duration {
    let exponent = retry_number.saturating_sub(1).min(63) as u32;
    let cap_ms = BASE_BACKOFF_MS
        .saturating_mul(2_u64.saturating_pow(exponent))
        .min(MAX_BACKOFF_MS);
    Duration::from_millis(rand::random_range(0..=cap_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn zero_retries_attempts_once() {
        let calls = AtomicUsize::new(0);
        let result = retry_with_backoff(
            0,
            |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>("transient")
            },
            |_| true,
            |_| Duration::ZERO,
        )
        .await;

        let failure = result.unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(failure.attempts, 1);
        assert_eq!(failure.reason, RetryStopReason::Exhausted);
    }

    #[tokio::test]
    async fn retriable_failure_recovers_on_retry() {
        let calls = AtomicUsize::new(0);
        let result = retry_with_backoff(
            2,
            |_| async {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    Err("transient")
                } else {
                    Ok("recovered")
                }
            },
            |_| true,
            |_| Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(result, ("recovered", 2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn non_retriable_failure_stops_immediately() {
        let calls = AtomicUsize::new(0);
        let result = retry_with_backoff(
            3,
            |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>("permanent")
            },
            |_| false,
            |_| Duration::ZERO,
        )
        .await;

        let failure = result.unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(failure.reason, RetryStopReason::NonRetriable);
    }

    #[tokio::test]
    async fn retriable_failure_exhausts_configured_attempts() {
        let calls = AtomicUsize::new(0);
        let result = retry_with_backoff(
            2,
            |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>("transient")
            },
            |_| true,
            |_| Duration::ZERO,
        )
        .await;

        let failure = result.unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(failure.attempts, 3);
        assert_eq!(failure.reason, RetryStopReason::Exhausted);
    }

    #[test]
    fn jitter_stays_within_exponential_cap() {
        for _ in 0..100 {
            assert!(full_jitter_backoff(1) <= Duration::from_millis(50));
            assert!(full_jitter_backoff(2) <= Duration::from_millis(100));
            assert!(full_jitter_backoff(10) <= Duration::from_secs(1));
        }
    }
}
