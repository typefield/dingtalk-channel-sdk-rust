//! # dingtalk-channel-sdk (Rust)
//!
//! DingTalk Channel SDK for Rust — a conversation access layer decoupled from
//! any agent runtime: Stream long connection, inbound event normalization, a
//! unified safety pipeline, and streaming AI-card replies, all behind one
//! high-level [`Channel`].
//!
//! Implements the shared multi-language channel contract (E1–E10 acceptance
//! checklist) also used by the Go, Java, Python and Node.js SDKs.
//!
//! ## Minimal example
//!
//! ```no_run
//! use dingtalk_channel::{Channel, Config};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ch = Channel::new(Config::new(
//!         std::env::var("DD_CLIENT_ID")?,
//!         std::env::var("DD_CLIENT_SECRET")?,
//!     ));
//!
//!     ch.on_message(|msg, reply| Box::pin(async move {
//!         let s = reply.stream().await?;
//!         for tok in fake_llm(&msg.text) {
//!             s.append(tok).await?;
//!         }
//!         s.finish(String::new()).await
//!     }));
//!
//!     ch.start().await?; // blocks; auto-reconnects
//!     Ok(())
//! }
//! # fn fake_llm(_: &str) -> Vec<String> { vec![] }
//! ```

pub mod bot_identity;
pub mod card;
pub mod channel;
pub mod config;
pub mod error;
pub mod frame;
pub mod http_mode;
pub mod lifecycle;
pub mod normalize;
pub mod oapi;
pub mod outbound;
pub mod pipeline;
pub mod ratelimit;
pub mod reply;
pub mod safety;
pub mod send;
pub mod stream;
pub mod token;
pub mod types;

pub use channel::{BatchHandler, CardActionHandler, Channel, MessageHandler};
pub use config::{
    Config, Transport, DEFAULT_API_BASE, DEFAULT_CARD_TEMPLATE_ID, DEFAULT_OAPI_BASE,
};
pub use error::{ApiError, Error, ErrorCode, Result};
pub use frame::Frame;
pub use lifecycle::LifecycleHooks;
pub use oapi::MediaUploadResult;
pub use reply::{CardStream, Reply};
pub use send::SendTarget;
pub use types::{
    AtUser, BatchConfig, BatchedMessage, BotIdentity, CardAction, DedupConfig, GroupOverride,
    IncomingMessage, MediaBatchConfig, Mention, OutboundConfig, PolicyConfig, PolicyDecision,
    RejectEvent, RejectReason, Resource, SafetyConfig,
};

/// Crate version reported in the User-Agent.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
