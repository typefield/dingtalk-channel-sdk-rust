//! Exponential-backoff outbound retry (Go `outbound/retry.go` port).
//!
//! Retries only errors classified as retryable (rate-limit / timeout / unknown);
//! format errors fail fast.

use crate::error::{classify_error, is_retryable, Error, Result};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct RetryOptions {
    pub max_attempts: usize,
    pub base_delay: Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
        }
    }
}

/// Execute `op` with exponential backoff (delay = base * 3^(attempt-1)).
pub async fn retry<Fut>(
    cancel: &CancellationToken,
    mut op: impl FnMut(usize) -> Fut,
    opts: &RetryOptions,
) -> Result<()>
where
    Fut: std::future::Future<Output = Result<()>>,
{
    let max = if opts.max_attempts == 0 {
        3
    } else {
        opts.max_attempts
    };
    let base = if opts.base_delay.is_zero() {
        Duration::from_millis(500)
    } else {
        opts.base_delay
    };
    let mut last_err: Option<Error> = None;
    for attempt in 1..=max {
        match op(attempt).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let classified = classify_error(&e);
                last_err = Some(classified);
                if attempt >= max || !is_retryable(last_err.as_ref().unwrap()) {
                    return Err(last_err.unwrap());
                }
                let delay = base.mul_f64(3f64.powi(attempt as i32 - 1));
                tokio::select! {
                    _ = cancel.cancelled() => return Err(Error::channel("context canceled")),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::channel("retry failed")))
}
