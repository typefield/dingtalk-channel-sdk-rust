//! Safety pipeline components: dedup cache, stale detector, processing lock,
//! policy gate, per-chat serial/batch queue (Go `internal/safety` port).

pub mod chat_queue;
pub mod policy_gate;
pub mod seen_cache;
pub mod ssrf_guard;
pub mod stale;

pub use chat_queue::ChatQueueManager;
pub use policy_gate::PolicyGate;
pub use seen_cache::{content_fingerprint, SeenCache};
pub use stale::{ProcessingLock, StaleDetector};
