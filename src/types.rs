//! Shared public types (SPEC §3/§4b; Go `types` package port).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Conversation type (raw DingTalk values "1"/"2" normalized).
pub const CONVERSATION_TYPE_DM: &str = "dm";
pub const CONVERSATION_TYPE_GROUP: &str = "group";

/// Default dedup TTL.
pub const DEFAULT_DEDUP_TTL: Duration = Duration::from_secs(5 * 60);

/// A user being @-mentioned in the raw payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AtUser {
    #[serde(default, rename = "dingtalkId")]
    pub dingtalk_id: String,
    #[serde(default, rename = "staffId")]
    pub staff_id: String,
}

/// Media resource carried by a message.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resource {
    /// image | file | audio | video
    #[serde(rename = "type")]
    pub r#type: String,
    /// DingTalk download code.
    #[serde(rename = "downloadCode")]
    pub download_code: String,
    /// File name (file/audio/video only).
    #[serde(rename = "fileName", default)]
    pub file_name: String,
    /// Speech recognition text (audio only).
    #[serde(rename = "recognition", default)]
    pub recognition: String,
}

/// An @-mention inside the message.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Mention {
    /// richText placeholder key.
    #[serde(rename = "key", default)]
    pub key: String,
    /// staffId or dingtalkId / mobile.
    #[serde(rename = "userId", default)]
    pub user_id: String,
    /// Display name.
    #[serde(rename = "name", default)]
    pub name: String,
    #[serde(rename = "isBot", default)]
    pub is_bot: bool,
    /// @所有人
    #[serde(rename = "isAll", default)]
    pub is_all: bool,
}

/// Normalized inbound bot message event (SPEC §3.1).
#[derive(Debug, Clone, Default, Serialize)]
pub struct IncomingMessage {
    #[serde(rename = "conversationId")]
    pub conversation_id: String,
    /// dm | group
    #[serde(rename = "conversationType")]
    pub conversation_type: String,
    #[serde(rename = "conversationTitle", default)]
    pub conversation_title: String,
    #[serde(rename = "senderId")]
    pub sender_id: String,
    #[serde(rename = "senderStaffId")]
    pub sender_staff_id: String,
    #[serde(rename = "senderNick")]
    pub sender_nick: String,
    #[serde(rename = "senderCorpId")]
    pub sender_corp_id: String,
    /// @-bot prefix stripped and trimmed text.
    pub text: String,
    #[serde(rename = "msgType")]
    pub msg_type: String,
    /// Raw rich content passthrough.
    #[serde(
        rename = "content",
        default,
        skip_serializing_if = "serde_json::Value::is_null"
    )]
    pub content: serde_json::Value,
    #[serde(rename = "resources", default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<Resource>,
    #[serde(rename = "mentions", default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<Mention>,
    #[serde(rename = "mentionAll", default)]
    pub mention_all: bool,
    #[serde(rename = "atUsers", default, skip_serializing_if = "Vec::is_empty")]
    pub at_users: Vec<AtUser>,
    /// Not serialized to avoid leaking secrets.
    #[serde(skip_serializing)]
    pub session_webhook: String,
    #[serde(rename = "webhookExpiredAt", default)]
    pub webhook_expired_at: i64,
    /// Business dedup key.
    #[serde(rename = "msgId")]
    pub msg_id: String,
    /// Event time (ms epoch).
    #[serde(rename = "createAt", default)]
    pub create_at: i64,
    #[serde(rename = "isAdmin", default)]
    pub is_admin: bool,
    #[serde(rename = "isInAtList", default)]
    pub is_in_at_list: bool,
    /// Original messages merged into this one (single-message dispatch has length 1).
    #[serde(skip)]
    pub batched_sources: Vec<std::sync::Arc<IncomingMessage>>,
    #[serde(
        rename = "raw",
        default,
        skip_serializing_if = "serde_json::Value::is_null"
    )]
    pub raw: serde_json::Value,
}

impl IncomingMessage {
    /// Whether the sessionWebhook is past its expiry.
    pub fn webhook_expired(&self) -> bool {
        self.webhook_expired_at > 0 && now_millis() > self.webhook_expired_at
    }
}

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Normalized card interaction callback (E7).
#[derive(Debug, Clone, Default)]
pub struct CardAction {
    pub out_track_id: String,
    pub user_id: String,
    pub data_content: serde_json::Value,
    pub raw: serde_json::Value,
}

/// Admission policy config (SPEC §4b).
#[derive(Debug, Clone, Default)]
pub struct PolicyConfig {
    /// Group allowlist (empty = allow all groups).
    pub group_allowlist: Vec<String>,
    /// Group blocklist.
    pub group_blocklist: Vec<String>,
    /// Group chats require @-mention of the bot (default true).
    pub require_mention: Option<bool>,
    /// Whether to respond to @all mentions (default false).
    pub respond_to_mention_all: Option<bool>,
    /// DM mode: open | disabled | allowlist | blocklist.
    pub dm_mode: String,
    /// DM allowlist (effective when dm_mode == "allowlist").
    pub dm_allowlist: Vec<String>,
    /// DM blocklist (effective when dm_mode == "blocklist").
    pub dm_blocklist: Vec<String>,
    /// Per-conversation policy overrides. Explicit entries can admit a group even
    /// under global allowlist mode; global blocklist is never exempted.
    pub group_overrides: HashMap<String, GroupOverride>,

    // Global sender controls.
    /// Global sender allowlist (staffId); when set, only listed senders pass.
    pub allow_from: Vec<String>,
    /// Global sender deny list (staffId); takes precedence over allow_from.
    pub deny_from: Vec<String>,
    /// Admins bypass all policy restrictions.
    pub admins: Vec<String>,
}

impl PolicyConfig {
    pub fn default_extended() -> Self {
        Self {
            dm_mode: "open".into(),
            ..Default::default()
        }
    }

    pub fn dm_mode(&self) -> &str {
        if self.dm_mode.is_empty() {
            "open"
        } else {
            &self.dm_mode
        }
    }
}

/// Per-group policy override.
#[derive(Debug, Clone, Default)]
pub struct GroupOverride {
    /// Explicitly disable the group (reject all its messages).
    pub enabled: Option<bool>,
    /// Override the group's @-mention requirement.
    pub require_mention: Option<bool>,
    /// Override the group's @all response behavior.
    pub respond_to_mention_all: Option<bool>,
    /// Sender allowlist within the group.
    pub allow_from: Vec<String>,
    /// Sender blocklist within the group (checked before allowlist).
    pub block_from: Vec<String>,
}

/// Policy evaluation result.
#[derive(Debug, Clone)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub reason: RejectReason,
}

impl PolicyDecision {
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reason: RejectReason::None,
        }
    }
    pub fn reject(reason: RejectReason) -> Self {
        Self {
            allowed: false,
            reason,
        }
    }
}

/// Rejection reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    None,
    GroupNotAllowed,
    GroupBlocked,
    GroupDisabled,
    NoMention,
    MentionAll,
    DmDisabled,
    DmNotAllowed,
    DmBlocked,
    SenderNotAllowed,
    SenderBlocked,
    Stale,
    Duplicate,
    SelfSent,
    LockContention,
    SenderDenied,
}

impl RejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RejectReason::None => "",
            RejectReason::GroupNotAllowed => "group_not_allowed",
            RejectReason::GroupBlocked => "group_blocked",
            RejectReason::GroupDisabled => "group_disabled",
            RejectReason::NoMention => "no_mention",
            RejectReason::MentionAll => "mention_all_blocked",
            RejectReason::DmDisabled => "dm_disabled",
            RejectReason::DmNotAllowed => "dm_not_allowed",
            RejectReason::DmBlocked => "dm_blocked",
            RejectReason::SenderNotAllowed => "sender_not_allowed",
            RejectReason::SenderBlocked => "sender_blocked",
            RejectReason::Stale => "stale",
            RejectReason::Duplicate => "duplicate",
            RejectReason::SelfSent => "self_sent",
            RejectReason::LockContention => "lock_contention",
            RejectReason::SenderDenied => "sender_denied",
        }
    }
}

/// Event emitted when a message is rejected by policy/safety stages.
#[derive(Debug, Clone)]
pub struct RejectEvent {
    pub message_id: String,
    pub chat_id: String,
    pub sender_id: String,
    pub reason: RejectReason,
}

/// Text batch processing config.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Batch debounce delay (default 600ms).
    pub delay_ms: u64,
    /// Long-message threshold in chars.
    pub long_threshold_chars: usize,
    /// Long-message delay (default 2s).
    pub long_delay_ms: u64,
    /// Max messages per batch (default 8).
    pub max_messages: usize,
    /// Max chars per batch (default 4000).
    pub max_chars: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            delay_ms: 600,
            long_threshold_chars: 1000,
            long_delay_ms: 2000,
            max_messages: 8,
            max_chars: 4000,
        }
    }
}

/// Batched message handed to OnBatch handlers.
#[derive(Debug, Clone)]
pub struct BatchedMessage {
    /// Merged message (last message as base, content concatenated).
    pub message: std::sync::Arc<IncomingMessage>,
    /// Source message IDs.
    pub source_ids: Vec<String>,
}

/// Per-chat serial queue config.
#[derive(Debug, Clone)]
pub struct ChatQueueConfig {
    /// Enable per-chat serialization (default true). When disabled replies may interleave.
    pub enabled: bool,
}

impl Default for ChatQueueConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Media batch merge config (default off).
#[derive(Debug, Clone)]
pub struct MediaBatchConfig {
    pub enabled: bool,
    /// Merge window in milliseconds (default 800).
    pub delay_ms: u64,
    /// Max items per batch (default 9).
    pub max_items: usize,
}

impl Default for MediaBatchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            delay_ms: 800,
            max_items: 9,
        }
    }
}

/// Outbound retry parameters (exponential backoff, factor 3 per attempt).
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub base_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 500,
        }
    }
}

/// Outbound hooks + unified footer.
#[derive(Clone, Default)]
pub struct OutboundConfig {
    pub retry: RetryConfig,
    /// Called before send with (kind, target, payload-as-json-string);
    /// returns replacement payload JSON string or None to keep.
    #[allow(clippy::type_complexity)]
    pub before_send:
        Option<std::sync::Arc<dyn Fn(&str, &str, &str) -> Option<String> + Send + Sync>>,
    /// Called after send with (kind, target, ok, err).
    #[allow(clippy::type_complexity)]
    pub after_send: Option<std::sync::Arc<dyn Fn(&str, &str, bool, &str) + Send + Sync>>,
    /// Unified footer appended to every text/markdown message (e.g. disclaimers).
    pub footer: String,
}

impl std::fmt::Debug for OutboundConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboundConfig")
            .field("retry", &self.retry)
            .field("footer", &self.footer)
            .finish_non_exhaustive()
    }
}

/// Bot identity info.
#[derive(Debug, Clone, Default)]
pub struct BotIdentity {
    pub robot_code: String,
    pub robot_name: String,
    pub avatar: String,
}

/// Dedup cache config.
#[derive(Debug, Clone)]
pub struct DedupConfig {
    pub ttl: Duration,
    pub max_entries: usize,
    #[allow(dead_code)]
    pub sweep_interval: Duration,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(12 * 3600),
            max_entries: 5000,
            sweep_interval: Duration::from_secs(5 * 60),
        }
    }
}

/// Unified safety pipeline configuration.
#[derive(Debug, Clone)]
pub struct SafetyConfig {
    pub dedup: DedupConfig,
    pub policy: PolicyConfig,
    pub text_batch: BatchConfig,
    pub media_batch: MediaBatchConfig,
    pub chat_queue: ChatQueueConfig,
    /// Stale message window (default 30 min).
    pub stale_window: Duration,
    /// Processing lock TTL (default 5 min).
    pub lock_ttl: Duration,
    /// Drop messages sent by the bot itself (default true).
    pub drop_self_sent: bool,
    /// Dedup marking semantics: false (default) marks at ingress;
    /// true marks only after successful handler completion (failed messages can be redelivered).
    pub mark_after_handler: bool,
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            dedup: DedupConfig::default(),
            policy: PolicyConfig::default_extended(),
            text_batch: BatchConfig::default(),
            media_batch: MediaBatchConfig::default(),
            chat_queue: ChatQueueConfig::default(),
            stale_window: Duration::from_secs(30 * 60),
            lock_ttl: Duration::from_secs(5 * 60),
            drop_self_sent: true,
            mark_after_handler: false,
        }
    }
}
