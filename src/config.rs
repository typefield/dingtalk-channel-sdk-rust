//! Channel configuration (SPEC; Go `config.go` port).

use crate::types::SafetyConfig;
use std::time::Duration;

pub const DEFAULT_API_BASE: &str = "https://api.dingtalk.com";
pub const DEFAULT_OAPI_BASE: &str = "https://oapi.dingtalk.com";
pub const DEFAULT_CARD_TEMPLATE_ID: &str = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema";
pub const DEFAULT_STREAM_THROTTLE: Duration = Duration::from_millis(800);
/// Orphan-card forced finish (connector-parity).
pub const DEFAULT_CARD_WATCHDOG: Duration = Duration::from_secs(10 * 60);
/// Error fallback text cooldown (anti-spam).
pub const DEFAULT_ERROR_COOLDOWN: Duration = Duration::from_secs(60);
/// Stale message window.
pub const DEFAULT_STALE_WINDOW: Duration = Duration::from_secs(30 * 60);
pub(crate) const DEFAULT_TEXT_CHUNK_LIMIT: usize = 3500;
pub(crate) const DEFAULT_CARD_QPS: f64 = 20.0;
pub(crate) const DEFAULT_QPS_BACKOFF: Duration = Duration::from_secs(2);
pub const DEFAULT_KEEP_ALIVE_IDLE: Duration = Duration::from_secs(120);
pub const DEFAULT_PONG_WAIT: Duration = Duration::from_secs(5);
pub const DEFAULT_RECONNECT_BASE: Duration = Duration::from_secs(1);
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(30);

pub const TOPIC_BOT_MESSAGE: &str = "/v1.0/im/bot/messages/get";
pub const TOPIC_CARD_INSTANCE_CB: &str = "/v1.0/card/instances/callback";

pub const USER_AGENT: &str = concat!("dingtalk-channel-sdk-rust/v", env!("CARGO_PKG_VERSION"));

/// Transport modes (official DingTalk receive modes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    /// Stream long connection (default, no public ingress).
    #[default]
    Stream,
    /// HTTP callback mode (signature verification built in).
    Http,
}

impl Transport {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "stream" => Some(Transport::Stream),
            "http" => Some(Transport::Http),
            _ => None,
        }
    }
}

pub(crate) const DEFAULT_HTTP_TIMESTAMP_TOLERANCE: Duration = Duration::from_secs(3600);

/// Full Channel configuration. `client_id`/`client_secret` are required;
/// everything else defaults sensibly (`Config::fill` semantics preserved via
/// resolved accessors so zero-values behave exactly like the Go SDK).
#[derive(Clone)]
pub struct Config {
    pub client_id: String,
    pub client_secret: String,

    /// Override default https://api.dingtalk.com (tests/private deployments).
    pub api_base: String,
    /// Override default https://oapi.dingtalk.com (media upload uses legacy OAPI).
    pub oapi_base: String,
    /// Override default AI card template.
    pub card_template_id: String,
    /// Minimum interval between card streaming updates (default 800ms).
    pub stream_throttle: Duration,
    /// Card watchdog: force-finish cards not finalized within this window
    /// (default 10min; zero disables).
    pub card_watchdog: Duration,
    /// Error fallback text cooldown per conversation (default 60s; zero disables).
    pub error_cooldown: Duration,
    /// Drop inbound messages older than this window (default 30min; zero disables).
    pub stale_message_window: Duration,
    /// Chunk oversized text/markdown replies at this many runes
    /// (default 3500; zero disables).
    pub text_chunk_limit: usize,
    /// Global token-bucket rate for card APIs (default 20 QPS).
    pub card_qps: f64,
    /// Auto-reconnect on disconnect (default true).
    pub auto_reconnect: bool,
    /// Idle duration before sending a ws ping (default 120s).
    pub keep_alive_idle: Duration,

    /// Unified safety configuration.
    pub safety: SafetyConfig,

    /// Allowlisted host names (exact or *.suffix); matched hosts skip public-URL checks.
    pub ssrf_allowlist: Vec<String>,

    /// Inbound transport mode (stream default / http).
    pub transport: Transport,
    /// HTTP mode signature timestamp tolerance (default 1h; zero disables check).
    pub http_timestamp_tolerance: Duration,

    /// Outbound configuration: retry params, BeforeSend/AfterSend hooks, unified footer.
    pub outbound: Option<std::sync::Arc<crate::types::OutboundConfig>>,

    /// Optional debug log hook.
    #[allow(clippy::type_complexity)]
    pub debug_log: Option<std::sync::Arc<dyn Fn(String) + Send + Sync>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            api_base: DEFAULT_API_BASE.into(),
            oapi_base: DEFAULT_OAPI_BASE.into(),
            card_template_id: DEFAULT_CARD_TEMPLATE_ID.into(),
            stream_throttle: DEFAULT_STREAM_THROTTLE,
            card_watchdog: DEFAULT_CARD_WATCHDOG,
            error_cooldown: DEFAULT_ERROR_COOLDOWN,
            stale_message_window: DEFAULT_STALE_WINDOW,
            text_chunk_limit: DEFAULT_TEXT_CHUNK_LIMIT,
            card_qps: DEFAULT_CARD_QPS,
            auto_reconnect: true,
            keep_alive_idle: DEFAULT_KEEP_ALIVE_IDLE,
            safety: SafetyConfig::default(),
            ssrf_allowlist: Vec::new(),
            transport: Transport::Stream,
            http_timestamp_tolerance: DEFAULT_HTTP_TIMESTAMP_TOLERANCE,
            outbound: None,
            debug_log: None,
        }
    }
}

impl Config {
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            ..Default::default()
        }
    }

    /// Effective stale window honoring the legacy `stale_message_window` override:
    /// if the legacy field is set (>0), it wins over safety.stale_window default.
    pub(crate) fn effective_stale_window(&self) -> Duration {
        if self.stale_message_window != DEFAULT_STALE_WINDOW
            && self.stale_message_window.as_millis() > 0
        {
            self.stale_message_window
        } else if self.safety.stale_window.is_zero() {
            DEFAULT_STALE_WINDOW
        } else {
            self.safety.stale_window
        }
    }

    pub(crate) fn debugf(&self, msg: impl std::fmt::Display) {
        if let Some(f) = &self.debug_log {
            f(msg.to_string());
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("client_id", &self.client_id)
            .field("api_base", &self.api_base)
            .field("oapi_base", &self.oapi_base)
            .field("card_template_id", &self.card_template_id)
            .field("stream_throttle", &self.stream_throttle)
            .field("card_watchdog", &self.card_watchdog)
            .field("error_cooldown", &self.error_cooldown)
            .field("stale_message_window", &self.stale_message_window)
            .field("text_chunk_limit", &self.text_chunk_limit)
            .field("card_qps", &self.card_qps)
            .field("auto_reconnect", &self.auto_reconnect)
            .field("keep_alive_idle", &self.keep_alive_idle)
            .field("safety", &self.safety)
            .field("transport", &self.transport)
            .field("outbound", &self.outbound)
            .finish_non_exhaustive()
    }
}
