//! Reply API: sessionWebhook text/markdown/image + AI-card streaming
//! (SPEC §4; Go `reply.go` port).

use crate::card::{CardClient, CardInstance, CardStreamer};
use crate::config::Config;
use crate::error::{classify_error, is_reply_target_gone, Error, Result};
use crate::oapi::{MediaUploadResult, OapiClient};
use crate::outbound::retry::RetryOptions;
use crate::token::TokenProvider;
use crate::types::IncomingMessage;
use futures_util::future::BoxFuture;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Streaming card handle exposed to handlers (SPEC §4).
pub trait CardStream: Send + Sync {
    /// Append a delta (throttled internally).
    fn append<'a>(&'a self, delta: String) -> BoxFuture<'a, Result<()>>;
    /// Finalize: final frame + FINISHED. Non-empty text overrides accumulation.
    fn finish<'a>(&'a self, text: String) -> BoxFuture<'a, Result<()>>;
    /// Mark FAILED and degrade to error text.
    fn fail<'a>(&'a self, err_text: String) -> BoxFuture<'a, Result<()>>;
    /// Explicit abort: seal stream, card → FAILED.
    fn abort<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
    /// Whether the card was really delivered (false = degraded webhook mode).
    fn card_delivered(&self) -> bool;
}

impl CardStream for CardStreamer {
    fn append<'a>(&'a self, delta: String) -> BoxFuture<'a, Result<()>> {
        Box::pin(CardStreamer::append(self, delta))
    }
    fn finish<'a>(&'a self, text: String) -> BoxFuture<'a, Result<()>> {
        Box::pin(CardStreamer::finish(self, text))
    }
    fn fail<'a>(&'a self, err_text: String) -> BoxFuture<'a, Result<()>> {
        Box::pin(CardStreamer::fail(self, err_text))
    }
    fn abort<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(CardStreamer::abort(self))
    }
    fn card_delivered(&self) -> bool {
        CardStreamer::card_delivered(self)
    }
}

/// Reply handle passed to message/card-action handlers.
/// text/markdown/image go through the sessionWebhook; `stream` uses AI cards.
pub trait Reply: Send + Sync {
    fn text(&self, content: String) -> BoxFuture<'static, Result<()>>;
    fn markdown(&self, title: String, text: String) -> BoxFuture<'static, Result<()>>;
    fn image(&self, image_url: String) -> BoxFuture<'static, Result<()>>;
    /// Immediately create and deliver an AI card (E1: "typing" card before first token).
    fn stream(&self) -> BoxFuture<'static, Result<Arc<dyn CardStream>>>;
    /// Exchange a download code for a media URL (E9).
    fn download_url(
        &self,
        download_code: String,
        msg_id: String,
    ) -> BoxFuture<'static, Result<String>>;
    /// Upload media via OAPI; returns mediaId (E9). media_type: image|file|video|voice.
    fn upload_media(
        &self,
        media_type: String,
        filename: String,
        content_type: String,
        data: Vec<u8>,
    ) -> BoxFuture<'static, Result<MediaUploadResult>>;
}

pub(crate) type ProactiveFn =
    dyn Fn(Arc<IncomingMessage>, String, Value) -> BoxFuture<'static, Result<()>> + Send + Sync;
pub(crate) type AfterSendHook = dyn Fn(&str, &str, bool, &str) + Send + Sync;

pub(crate) struct Replier {
    pub msg: Arc<IncomingMessage>,
    pub cfg: Arc<Config>,
    pub tokens: Arc<TokenProvider>,
    pub cards: Arc<CardClient>,
    pub oapi: Arc<OapiClient>,
    pub http: reqwest::Client,
    /// Proactive-send fallback when the webhook is expired/revoked/missing.
    pub proactive: Option<Arc<ProactiveFn>>,
    /// Outbound config: footer + BeforeSend hook.
    pub outbound: Option<Arc<crate::types::OutboundConfig>>,
    /// AfterSend hook (kind, target, ok, err).
    pub after_send_hook: Option<Arc<AfterSendHook>>,
}

impl Replier {
    async fn deliver_once(&self, msg_key: &str, msg_param: Value) -> Result<()> {
        match self.webhook_once(msg_key, &msg_param).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let classified = classify_error(&e);
                if let Some(proactive) = &self.proactive {
                    if is_reply_target_gone(&classified) {
                        self.cfg.debugf(format!(
                            "reply webhook target gone ({e}), falling back to proactive send"
                        ));
                        return (proactive)(self.msg.clone(), msg_key.to_string(), msg_param).await;
                    }
                }
                Err(e)
            }
        }
    }

    async fn webhook_once(&self, msg_key: &str, msg_param: &Value) -> Result<()> {
        let opts = RetryOptions {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
        };
        let cancel = CancellationToken::new();
        let this = Arc::new(Self {
            msg: self.msg.clone(),
            cfg: self.cfg.clone(),
            tokens: self.tokens.clone(),
            cards: self.cards.clone(),
            oapi: self.oapi.clone(),
            http: self.http.clone(),
            proactive: None,
            outbound: None,
            after_send_hook: None,
        });
        let key = msg_key.to_string();
        let param = msg_param.clone();
        let res = crate::outbound::retry(
            &cancel,
            move |_| {
                let this = this.clone();
                let key = key.clone();
                let param = param.clone();
                async move { this.webhook_do(&key, &param).await }
            },
            &opts,
        )
        .await;
        match &res {
            Ok(()) => self.after_send(true, ""),
            Err(e) => self.after_send(false, &e.to_string()),
        }
        res
    }

    fn after_send(&self, ok: bool, err: &str) {
        // Outbound AfterSend hook is wired by Channel when configured.
        if let Some(hook) = &self.after_send_hook {
            hook("reply", &self.msg.conversation_id, ok, err);
        }
    }

    async fn webhook_do(&self, msg_key: &str, msg_param: &Value) -> Result<()> {
        // Official docs require msgParam to be stringified JSON (object form 400s).
        let body = json!({"msgKey": msg_key, "msgParam": msg_param.to_string()});
        let token = self.tokens.get().await?;
        let resp = self
            .http
            .post(self.msg.session_webhook.as_str())
            .header("Content-Type", "application/json")
            .header("x-acs-dingtalk-access-token", token)
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let raw = resp.text().await?;
        if status >= 400 {
            return Err(Error::Api(crate::error::ApiError {
                status,
                code: String::new(),
                msg: String::new(),
                body: raw,
            }));
        }
        Ok(())
    }

    async fn webhook(
        &self,
        msg_key: &str,
        mut msg_param: serde_json::Map<String, Value>,
    ) -> Result<()> {
        // Outbound footer + BeforeSend hook.
        self.apply_outbound_config(msg_key, &mut msg_param);

        // Expired or missing webhook → proactive fallback directly.
        if self.msg.session_webhook.is_empty() || self.msg.webhook_expired() {
            if self.msg.session_webhook.is_empty() && self.proactive.is_none() {
                return Err(Error::channel("reply: sessionWebhook missing"));
            }
            if let Some(proactive) = &self.proactive {
                self.cfg.debugf(
                    "reply webhook unavailable (missing/expired), falling back to proactive send",
                );
                return (proactive)(
                    self.msg.clone(),
                    msg_key.to_string(),
                    Value::Object(msg_param),
                )
                .await;
            }
        }

        // Oversized text/markdown: chunk at TextChunkLimit so nothing is truncated.
        if self.cfg.text_chunk_limit > 0 {
            if let Some(content) = msg_param
                .get("content")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
            {
                for c in crate::outbound::splitter::chunk_text(&content, self.cfg.text_chunk_limit)
                {
                    let mut p = serde_json::Map::new();
                    p.insert("content".into(), Value::String(c));
                    self.deliver_once(msg_key, Value::Object(p)).await?;
                }
                return Ok(());
            }
            if let Some(text) = msg_param
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
            {
                let title = msg_param
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                for c in crate::outbound::splitter::chunk_text(&text, self.cfg.text_chunk_limit) {
                    let mut p = serde_json::Map::new();
                    p.insert("title".into(), Value::String(title.clone()));
                    p.insert("text".into(), Value::String(c));
                    self.deliver_once(msg_key, Value::Object(p)).await?;
                }
                return Ok(());
            }
        }
        self.deliver_once(msg_key, Value::Object(msg_param)).await
    }

    fn apply_outbound_config(&self, msg_key: &str, param: &mut serde_json::Map<String, Value>) {
        let Some(outbound) = &self.outbound else {
            return;
        };
        if !outbound.footer.is_empty() {
            if msg_key == "sampleText" {
                if let Some(c) = param
                    .get_mut("content")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                {
                    param.insert(
                        "content".into(),
                        Value::String(format!("{c}\n\n{}", outbound.footer)),
                    );
                }
            } else if msg_key == "sampleMarkdown" {
                if let Some(t) = param
                    .get_mut("text")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                {
                    param.insert(
                        "text".into(),
                        Value::String(format!("{t}\n\n---\n{}", outbound.footer)),
                    );
                }
            }
        }
        if let Some(before) = &outbound.before_send {
            let payload = Value::Object(param.clone()).to_string();
            if let Some(replaced) = before("reply", &self.msg.conversation_id, &payload) {
                if let Ok(Value::Object(m)) = serde_json::from_str::<Value>(&replaced) {
                    *param = m;
                }
            }
        }
    }
}

// Extra fields carried outside the struct definition for clarity.

impl Reply for Replier {
    fn text(&self, content: String) -> BoxFuture<'static, Result<()>> {
        let this = self.clone_arc();
        Box::pin(async move {
            let mut p = serde_json::Map::new();
            p.insert("content".into(), Value::String(content));
            this.webhook("sampleText", p).await
        })
    }

    fn markdown(&self, title: String, text: String) -> BoxFuture<'static, Result<()>> {
        let this = self.clone_arc();
        Box::pin(async move {
            let mut title = title;
            if title.is_empty() {
                title = first_line_title(&text);
            }
            let mut p = serde_json::Map::new();
            p.insert("title".into(), Value::String(title));
            p.insert("text".into(), Value::String(text));
            this.webhook("sampleMarkdown", p).await
        })
    }

    fn image(&self, image_url: String) -> BoxFuture<'static, Result<()>> {
        let this = self.clone_arc();
        Box::pin(async move {
            let mut p = serde_json::Map::new();
            p.insert("photoURL".into(), Value::String(image_url));
            this.webhook("sampleImageMsg", p).await
        })
    }

    fn stream(&self) -> BoxFuture<'static, Result<Arc<dyn CardStream>>> {
        let this = self.clone_arc();
        Box::pin(async move {
            let cancel = CancellationToken::new();
            let target = crate::card::CardTarget {
                is_group: this.msg.conversation_type == crate::types::CONVERSATION_TYPE_GROUP,
                conversation_id: this.msg.conversation_id.clone(),
                user_id: first_non_empty([&this.msg.sender_staff_id, &this.msg.sender_id]),
                robot_code: this.cfg.client_id.clone(),
            };

            // Error-cooldown-guarded fallback (anti error-text spam).
            let conv_id = this.msg.conversation_id.clone();
            let cooldown = this.cfg.error_cooldown;
            let fallback_this = this.clone_arc();
            let fallback: crate::card::FallbackFn = Arc::new(move |text| {
                let this = fallback_this.clone();
                let conv_id = conv_id.clone();
                Box::pin(async move {
                    if !error_cooldown_pass(&conv_id, cooldown) {
                        return Ok(()); // within cooldown: suppress duplicate error texts
                    }
                    this.text(text).await
                })
            });
            let rest_this = this.clone_arc();
            let deliver_rest: crate::card::DeliverRestFn = Arc::new(move |text| {
                let this = rest_this.clone();
                Box::pin(async move {
                    if let Err(e) = this.text(text).await {
                        this.cfg
                            .debugf(format!("card overflow remainder deliver failed: {e}"));
                    }
                })
            });

            let create = this.cards.create_and_deliver(&cancel, &target).await;
            let card: Option<CardInstance> = match create {
                Ok(c) => {
                    this.cfg.debugf("card created");
                    Some(c)
                }
                Err(e) => {
                    // Silent degradation: streamer stays usable, updates go via webhook (E4).
                    this.cfg
                        .debugf(format!("card create failed, fallback to webhook text: {e}"));
                    None
                }
            };
            let streamer = CardStreamer::new(
                this.cards.clone(),
                card,
                this.cfg.stream_throttle,
                this.cfg.card_watchdog,
                cancel,
                Some(fallback),
                Some(deliver_rest),
            );
            Ok(Arc::new(streamer) as Arc<dyn CardStream>)
        })
    }

    fn download_url(
        &self,
        download_code: String,
        msg_id: String,
    ) -> BoxFuture<'static, Result<String>> {
        let this = self.clone_arc();
        Box::pin(async move {
            let path = format!(
                "/v1.0/robot/messageFiles/download?downloadCode={}&messageId={}&robotCode={}",
                urlencoding::encode(&download_code),
                urlencoding::encode(&msg_id),
                urlencoding::encode(&this.cfg.client_id),
            );
            let cancel = CancellationToken::new();
            let raw = this
                .cards
                .call_raw_public(&cancel, reqwest::Method::GET, &path)
                .await?;
            let v: Value = serde_json::from_str(&raw)?;
            Ok(v.get("downloadUrl")
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .to_string())
        })
    }

    fn upload_media(
        &self,
        media_type: String,
        filename: String,
        content_type: String,
        data: Vec<u8>,
    ) -> BoxFuture<'static, Result<MediaUploadResult>> {
        let this = self.clone_arc();
        Box::pin(async move {
            this.oapi
                .upload_media(&media_type, &filename, &content_type, data)
                .await
        })
    }
}

impl Replier {
    fn clone_arc(self: &Replier) -> Arc<Replier> {
        // Replier fields are all Arc'd; a lightweight clone wrapper keeps
        // `Reply for Replier` object-safe while handing 'static futures.
        Arc::new(Replier {
            msg: self.msg.clone(),
            cfg: self.cfg.clone(),
            tokens: self.tokens.clone(),
            cards: self.cards.clone(),
            oapi: self.oapi.clone(),
            http: self.http.clone(),
            proactive: self.proactive.clone(),
            outbound: self.outbound.clone(),
            after_send_hook: self.after_send_hook.clone(),
        })
    }
}

/// Expose call_raw for DownloadURL without widening CardClient's API surface.
impl CardClient {
    pub(crate) async fn call_raw_public(
        &self,
        cancel: &CancellationToken,
        method: reqwest::Method,
        path: &str,
    ) -> Result<String> {
        self.call_raw(cancel, method, path, None).await
    }
}

pub(crate) fn first_non_empty<'a>(vals: impl IntoIterator<Item = &'a String>) -> String {
    for v in vals {
        if !v.is_empty() {
            return v.clone();
        }
    }
    String::new()
}

/// Derive a markdown title from the first meaningful line (≤20 chars).
pub(crate) fn first_line_title(text: &str) -> String {
    for line in text.split('\n') {
        let t = line.trim_start_matches(['#', '*', '-', '>', ' ', '\t']);
        if !t.is_empty() {
            let t: String = t.chars().take(20).collect();
            return t;
        }
    }
    "Message".to_string()
}

// ── Error-fallback cooldown (connector parity: one error text per conversation per window) ──

fn error_cooldown_pass(key: &str, cooldown: Duration) -> bool {
    static LAST: std::sync::Mutex<Option<HashMap<String, Instant>>> = std::sync::Mutex::new(None);
    if cooldown.is_zero() {
        return true;
    }
    let mut guard = LAST.lock().unwrap();
    let map = guard.get_or_insert_with(HashMap::new);
    let now = Instant::now();
    if let Some(t) = map.get(key) {
        if now.duration_since(*t) < cooldown {
            return false;
        }
    }
    if map.len() > 4096 {
        map.clear(); // unbounded-growth guard; cooldown is best-effort anti-spam
    }
    map.insert(key.to_string(), now);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::card::{overflow_remainder, CARD_MAX_CONTENT};

    #[test]
    fn title_from_first_line() {
        assert_eq!(first_line_title("# Hello\nbody"), "Hello");
        assert_eq!(first_line_title(""), "Message");
        let long = first_line_title("this is a very long title that exceeds twenty chars");
        assert!(long.chars().count() <= 20);
    }

    #[test]
    fn cooldown_allows_first_blocks_second() {
        let key = "conv-x";
        assert!(error_cooldown_pass(key, Duration::from_secs(60)));
        assert!(!error_cooldown_pass(key, Duration::from_secs(60)));
        assert!(error_cooldown_pass("other-conv", Duration::from_secs(60)));
    }

    #[test]
    fn overflow_remainder_works() {
        let s: String = "中".repeat(CARD_MAX_CONTENT + 5);
        let rest = overflow_remainder(&s).unwrap();
        assert_eq!(rest.chars().count(), 5);
        assert!(overflow_remainder("short").is_none());
    }
}
