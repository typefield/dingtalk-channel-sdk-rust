//! Stale message detector + short-TTL processing lock
//! (Go `stale_detector.go` / `processing_lock.go` ports).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Filters gateway-redelivered stale messages (e.g. history replay after reconnect).
#[derive(Debug, Clone)]
pub struct StaleDetector {
    window: Duration,
}

impl StaleDetector {
    pub fn new(window: Duration) -> Self {
        Self {
            window: if window.is_zero() {
                Duration::from_secs(30 * 60)
            } else {
                window
            },
        }
    }

    /// `create_at_ms == 0` means unknown time → conservatively pass.
    pub fn is_stale(&self, create_at_ms: i64) -> bool {
        if create_at_ms <= 0 {
            return false;
        }
        let created = std::time::UNIX_EPOCH + Duration::from_millis(create_at_ms as u64);
        match created.elapsed() {
            Ok(age) => age > self.window,
            Err(_) => false, // clock skew into the future: pass
        }
    }
}

/// Short-TTL in-memory lock preventing concurrent processing of one event.
pub struct ProcessingLock {
    ttl: Duration,
    locks: Mutex<HashMap<String, Instant>>,
}

impl ProcessingLock {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire the lock for `id`; false when already held.
    pub fn acquire(&self, id: &str) -> bool {
        let mut locks = self.locks.lock().unwrap();
        let now = Instant::now();
        if let Some(exp) = locks.get(id) {
            if now < *exp {
                return false;
            }
        }
        locks.insert(id.to_string(), now + self.ttl);
        true
    }

    pub fn release(&self, id: &str) {
        self.locks.lock().unwrap().remove(id);
    }

    /// Drop expired entries.
    pub fn sweep(&self) {
        let mut locks = self.locks.lock().unwrap();
        let now = Instant::now();
        locks.retain(|_, exp| *exp > now);
    }
}
