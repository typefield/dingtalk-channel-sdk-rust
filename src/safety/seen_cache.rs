//! Dedup cache: multi-key check-and-mark with TTL + LRU eviction
//! (Go `safety/seen_cache.go` port).

use crate::types::DedupConfig;
use lru::LruCache;
use sha2::{Digest, Sha256};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Instant;

struct Entry {
    expires_at: Instant,
}

/// Multi-key dedup cache. Any key hit marks the message as duplicate.
pub struct SeenCache {
    cfg: DedupConfig,
    map: Mutex<LruCache<String, Entry>>,
}

impl SeenCache {
    pub fn new(cfg: DedupConfig) -> Self {
        let max = NonZeroUsize::new(cfg.max_entries.max(1)).unwrap();
        Self {
            cfg,
            map: Mutex::new(LruCache::new(max)),
        }
    }

    /// Check whether any of `keys` was seen and is still fresh (LRU refresh on hit).
    pub fn has(&self, keys: &[&str]) -> bool {
        let mut map = self.map.lock().unwrap();
        let now = Instant::now();
        for key in keys {
            if key.is_empty() {
                continue;
            }
            match map.get(*key) {
                Some(entry) if now < entry.expires_at => return true,
                // Expired entries are dropped by `get` returning a reference we
                // can't keep; explicitly pop stale ones.
                Some(_) => {
                    map.pop(*key);
                }
                None => {}
            }
        }
        false
    }

    /// Add all keys with the configured TTL.
    pub fn add(&self, keys: &[&str]) {
        let mut map = self.map.lock().unwrap();
        let expires_at = Instant::now() + self.cfg.ttl;
        for key in keys {
            if key.is_empty() {
                continue;
            }
            map.put((*key).to_string(), Entry { expires_at });
        }
    }

    /// Atomic check + mark. Returns true when the message is a duplicate.
    pub fn check_and_mark(&self, keys: &[&str]) -> bool {
        if self.has(keys) {
            return true;
        }
        self.add(keys);
        false
    }

    /// Remove all entries (test helper / dispose).
    pub fn clear(&self) {
        self.map.lock().unwrap().clear();
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Content fingerprint (SHA-256 over conversationId:createAt:msgType:content).
pub fn content_fingerprint(
    conversation_id: &str,
    create_at: i64,
    msg_type: &str,
    content: &str,
) -> String {
    let data = format!("{conversation_id}:{create_at}:{msg_type}:{content}");
    let hash = Sha256::digest(data.as_bytes());
    format!("fp:{hash:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn dedup_roundtrip() {
        let c = SeenCache::new(DedupConfig {
            ttl: Duration::from_millis(50),
            ..Default::default()
        });
        assert!(!c.check_and_mark(&["a", "b"]));
        assert!(c.check_and_mark(&["b"])); // second key hits
        assert!(c.check_and_mark(&["a"]));
        assert!(!c.has(&["c"]));
        c.add(&["c"]);
        assert!(c.has(&["c"]));
    }

    #[test]
    fn dedup_ttl_expiry() {
        let c = SeenCache::new(DedupConfig {
            ttl: Duration::from_millis(20),
            max_entries: 10,
            sweep_interval: Default::default(),
        });
        c.add(&["k"]);
        assert!(c.has(&["k"]));
        std::thread::sleep(Duration::from_millis(30));
        assert!(!c.has(&["k"]));
    }

    #[test]
    fn fingerprint_shape() {
        let fp = content_fingerprint("cid", 42, "text", "hi");
        assert!(fp.starts_with("fp:"));
        let fp2 = content_fingerprint("cid", 42, "text", "hi");
        assert_eq!(fp, fp2);
        let fp3 = content_fingerprint("cid", 43, "text", "hi");
        assert_ne!(fp, fp3);
    }
}
