//! Global token bucket for card APIs + QpsLimit backoff (SPEC §6; Go `ratelimit.go` port).

use crate::config::DEFAULT_QPS_BACKOFF;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Token bucket. [`TokenBucket::wait_for`] blocks until a token is available;
/// [`TokenBucket::trigger_backoff`] pauses all acquirers for 2s.
pub struct TokenBucket {
    rate: f64,
    state: std::sync::Mutex<BucketState>,
}

#[derive(Debug, Clone, Copy)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    backoff_end: Option<Instant>,
}

impl TokenBucket {
    pub fn new(rate: f64) -> Self {
        Self {
            rate,
            state: std::sync::Mutex::new(BucketState {
                tokens: rate,
                last_refill: Instant::now(),
                backoff_end: None,
            }),
        }
    }

    fn refill_locked(&self, st: &mut BucketState, now: Instant) {
        let elapsed = now.duration_since(st.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            st.tokens = (st.tokens + elapsed * self.rate).min(self.rate);
            st.last_refill = now;
        }
    }

    /// Acquire one token, blocking as needed. Returns actual wait duration.
    pub async fn wait_for(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Duration, crate::error::Error> {
        let start = Instant::now();
        loop {
            let sleep_for = {
                let mut st = self.state.lock().unwrap();
                let now = Instant::now();
                if let Some(end) = st.backoff_end {
                    if now < end {
                        end - now
                    } else {
                        st.backoff_end = None;
                        Duration::ZERO
                    }
                } else {
                    Duration::ZERO
                }
            };
            if sleep_for > Duration::ZERO {
                if cancel.is_cancelled() || sleep_cancellable(sleep_for, cancel).await.is_err() {
                    return Err(crate::error::Error::channel("context canceled"));
                }
                continue;
            }

            let need = {
                let mut st = self.state.lock().unwrap();
                let now = Instant::now();
                self.refill_locked(&mut st, now);
                if st.tokens >= 1.0 {
                    st.tokens -= 1.0;
                    None
                } else {
                    Some(Duration::from_secs_f64((1.0 - st.tokens) / self.rate))
                }
            };
            match need {
                None => return Ok(start.elapsed()),
                Some(d) => {
                    let d = d.max(Duration::from_millis(1));
                    if cancel.is_cancelled() || sleep_cancellable(d, cancel).await.is_err() {
                        return Err(crate::error::Error::channel("context canceled"));
                    }
                }
            }
        }
    }

    /// Clear tokens and pause for 2s (QpsLimit reaction).
    pub fn trigger_backoff(&self) {
        let mut st = self.state.lock().unwrap();
        let end = Instant::now() + DEFAULT_QPS_BACKOFF;
        st.backoff_end = Some(end);
        st.tokens = 0.0;
        st.last_refill = end;
    }
}

async fn sleep_cancellable(d: Duration, cancel: &CancellationToken) -> Result<(), ()> {
    tokio::select! {
        _ = tokio::time::sleep(d) => Ok(()),
        _ = cancel.cancelled() => Err(()),
    }
}
