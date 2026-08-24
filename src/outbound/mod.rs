//! Outbound processing: markdown normalization for the AI-card renderer,
//! code-fence-aware text splitting, and exponential-backoff retry.

pub mod markdown;
pub mod retry;
pub mod splitter;

pub use markdown::normalize_for_card;
pub use retry::{retry, RetryOptions};
pub use splitter::split_with_code_fences;
