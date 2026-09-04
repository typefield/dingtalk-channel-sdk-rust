//! High-level Channel: wires stream transport, safety pipeline, reply/send
//! components and user handlers into one entry point (Go `channel.go` port).
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
//!     ch.on_message(|msg, reply| Box::pin(async move {
//!         reply.text(format!("received: {}", msg.text)).await
//!     }));
//!     ch.start().await?;
//!     Ok(())
//! }
//! ```

use crate::bot_identity::BotIdentityProvider;
use crate::card::CardClient;
use crate::config::{Config, Transport};
use crate::error::{Error, Result};
use crate::frame::Frame;
use crate::lifecycle::LifecycleHooks;
use crate::oapi::OapiClient;
use crate::pipeline::{
    BatchDispatchFn, MessageDispatchFn, PipelineOptions, RejectFn, SafetyPipeline,
};
use crate::ratelimit::TokenBucket;
use crate::reply::{ProactiveFn, Replier};
use crate::safety::chat_queue::ChatQueueManager;
use crate::send::ProactiveSender;
use crate::stream::StreamConn;
use crate::token::TokenProvider;
use crate::types::{BotIdentity, CardAction, IncomingMessage, PolicyConfig, RejectEvent};
use futures_util::future::BoxFuture;
use std::sync::{Arc, RwLock};

/// Business message handler: only cares about "what did the user say, what
/// does the bot reply".
pub type MessageHandler = dyn Fn(Arc<IncomingMessage>, Arc<dyn crate::reply::Reply>) -> BoxFuture<'static, Result<()>>
    + Send
    + Sync;

/// Card interaction handler (E7).
pub type CardActionHandler = dyn Fn(CardAction, Arc<dyn crate::reply::Reply>) -> BoxFuture<'static, Result<()>>
    + Send
    + Sync;

/// Batched-message handler (OnBatch path).
pub type BatchHandler =
    dyn Fn(crate::types::BatchedMessage) -> BoxFuture<'static, Result<()>> + Send + Sync;

type OnBatchSlot = RwLock<Option<Arc<BatchHandler>>>;

pub(crate) struct Core {
    pub cfg: Arc<Config>,
    pub tokens: Arc<TokenProvider>,
    pub cards: Arc<CardClient>,
    pub oapi: Arc<OapiClient>,
    pub sender: ProactiveSender,
    pub http: reqwest::Client,
    pub hooks: Arc<LifecycleHooks>,

    pub on_message: RwLock<Option<Arc<MessageHandler>>>,
    pub on_card_action: RwLock<Option<Arc<CardActionHandler>>>,
    pub on_batch: OnBatchSlot,
    pub on_reject: RwLock<Option<RejectFn>>,
}

impl Core {
    /// Build a Replier bound to one inbound message.
    pub(crate) fn make_replier(&self, msg: Arc<IncomingMessage>) -> Arc<Replier> {
        let sender = self.sender.clone();
        let proactive: Arc<ProactiveFn> = Arc::new(move |msg, msg_key, msg_param| {
            let sender = sender.clone();
            Box::pin(async move { sender.reply_fallback(&msg, &msg_key, msg_param).await })
        });
        Arc::new(Replier {
            msg,
            cfg: self.cfg.clone(),
            tokens: self.tokens.clone(),
            cards: self.cards.clone(),
            oapi: self.oapi.clone(),
            http: self.http.clone(),
            proactive: Some(proactive),
            outbound: self.cfg.outbound.clone(),
            after_send_hook: self
                .cfg
                .outbound
                .as_ref()
                .and_then(|o| o.after_send.clone()),
        })
    }
}

/// DingTalk conversation access layer.
pub struct Channel {
    pub(crate) core: Arc<Core>,
    conn: Arc<StreamConn>,
    bot_identity: Arc<BotIdentityProvider>,
    pipeline: Arc<SafetyPipeline>,
}

impl Channel {
    /// Create a Channel. Config defaults are applied on first use.
    pub fn new(cfg: Config) -> Arc<Self> {
        let cfg = Arc::new(cfg);
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        let tokens = Arc::new(TokenProvider::new(cfg.clone(), http.clone()));
        let bucket = Arc::new(TokenBucket::new(if cfg.card_qps <= 0.0 {
            20.0
        } else {
            cfg.card_qps
        }));
        let cards = Arc::new(CardClient::new(
            cfg.clone(),
            tokens.clone(),
            bucket,
            http.clone(),
        ));
        let oapi = Arc::new(OapiClient::new(cfg.clone(), http.clone()));
        let sender = ProactiveSender::new(cfg.clone(), cards.clone());
        let hooks = Arc::new(LifecycleHooks::new());

        let core = Arc::new(Core {
            cfg: cfg.clone(),
            tokens,
            cards,
            oapi,
            sender,
            http: http.clone(),
            hooks: hooks.clone(),
            on_message: RwLock::new(None),
            on_card_action: RwLock::new(None),
            on_batch: RwLock::new(None),
            on_reject: RwLock::new(None),
        });

        // Per-chat serial + batch queue (messages and card callbacks share it).
        let chat_queue = Arc::new(ChatQueueManager::new(
            cfg.safety.text_batch.clone(),
            cfg.safety.chat_queue.enabled,
            cfg.safety.media_batch.clone(),
        ));

        // Legacy field override parity: an explicitly set stale_message_window
        // wins over safety.stale_window.
        let mut safety_cfg = cfg.safety.clone();
        safety_cfg.stale_window = cfg.effective_stale_window();

        // Safety pipeline: closures read the latest handler registrations from
        // core so handlers registered after construction still take effect.
        let weak_core = Arc::downgrade(&core);
        let on_message_dispatch: MessageDispatchFn = Arc::new(move |msg, _sources| {
            let core = weak_core.clone();
            Box::pin(async move {
                let Some(core) = core.upgrade() else {
                    return Ok(());
                };
                let handler = core.on_message.read().unwrap().clone();
                match handler {
                    None => Ok(()),
                    Some(h) => {
                        let reply = core.make_replier(msg.clone());
                        h(msg, reply as Arc<dyn crate::reply::Reply>).await
                    }
                }
            })
        });
        let weak_core2 = Arc::downgrade(&core);
        let on_batch_dispatch: BatchDispatchFn = Arc::new(move |batch| {
            let core = weak_core2.clone();
            Box::pin(async move {
                let Some(core) = core.upgrade() else {
                    return Ok(());
                };
                let handler = core.on_batch.read().unwrap().clone();
                match handler {
                    None => Ok(()),
                    Some(h) => h(batch).await,
                }
            })
        });
        let weak_core3 = Arc::downgrade(&core);
        let on_reject_dispatch: RejectFn = Arc::new(move |event| {
            if let Some(core) = weak_core3.upgrade() {
                if let Some(h) = core.on_reject.read().unwrap().as_ref() {
                    h(event);
                }
            }
        });

        let pipeline = Arc::new(SafetyPipeline::new(
            safety_cfg,
            PipelineOptions {
                on_message: on_message_dispatch,
                on_batch: Some(on_batch_dispatch),
                has_on_batch: Box::new({
                    let core = core.clone();
                    move || core.on_batch.read().unwrap().is_some()
                }),
                on_reject: Some(on_reject_dispatch),
                chat_queue,
                bot_robot_code: String::new(),
            },
        ));

        // Frame dispatcher.
        let weak_core4 = Arc::downgrade(&core);
        let weak_pipeline = Arc::downgrade(&pipeline);
        let on_frame: crate::stream::OnFrameFn = Arc::new(move |f: Frame| {
            let core = weak_core4.clone();
            let pipeline = weak_pipeline.clone();
            Box::pin(async move {
                if let (Some(core), Some(pipeline)) = (core.upgrade(), pipeline.upgrade()) {
                    dispatch_frame(&core, &pipeline, &f).await;
                }
                String::new()
            })
        });

        let conn = Arc::new(StreamConn::new(cfg.clone(), http, on_frame, hooks));
        let bot_identity = Arc::new(BotIdentityProvider::new(
            cfg.clone(),
            core.http.clone(),
            core.tokens.clone(),
        ));

        Arc::new(Self {
            core,
            conn,
            bot_identity,
            pipeline,
        })
    }

    // ── Handler registration ──

    /// Register the message handler (unified DM/group entry, E5).
    pub fn on_message<F>(&self, f: F)
    where
        F: Fn(Arc<IncomingMessage>, Arc<dyn crate::reply::Reply>) -> BoxFuture<'static, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        *self.core.on_message.write().unwrap() = Some(Arc::new(f));
    }

    /// Register the card interaction handler; registering auto-subscribes the
    /// card callback topic.
    pub fn on_card_action<F>(&self, f: F)
    where
        F: Fn(CardAction, Arc<dyn crate::reply::Reply>) -> BoxFuture<'static, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        *self.core.on_card_action.write().unwrap() = Some(Arc::new(f));
        self.conn.set_card_topic_wanted(true);
    }

    /// Register the reject-event callback (full observability of dropped messages).
    pub fn on_reject<F>(&self, f: F)
    where
        F: Fn(RejectEvent) + Send + Sync + 'static,
    {
        *self.core.on_reject.write().unwrap() = Some(Arc::new(f));
    }

    /// Register the batch handler. After registration messages are grouped per
    /// conversation and delivered merged instead of one-by-one.
    pub fn on_batch_message<F>(&self, f: F)
    where
        F: Fn(crate::types::BatchedMessage) -> BoxFuture<'static, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        *self.core.on_batch.write().unwrap() = Some(Arc::new(f));
    }

    // ── Lifecycle hooks ──

    pub fn on_ready(&self, f: impl Fn() + Send + Sync + 'static) {
        self.core.hooks.on_ready(f);
    }
    pub fn on_error(&self, f: impl Fn(&Error) + Send + Sync + 'static) {
        self.core.hooks.on_error(f);
    }
    pub fn on_reconnecting(&self, f: impl Fn() + Send + Sync + 'static) {
        self.core.hooks.on_reconnecting(f);
    }
    pub fn on_reconnected(&self, f: impl Fn() + Send + Sync + 'static) {
        self.core.hooks.on_reconnected(f);
    }
    pub fn on_disconnected(&self, f: impl Fn() + Send + Sync + 'static) {
        self.core.hooks.on_disconnected(f);
    }

    // ── Policy ──

    pub fn update_policy(&self, cfg: PolicyConfig) {
        self.pipeline.update_policy(cfg);
    }

    pub fn get_policy(&self) -> PolicyConfig {
        self.pipeline.get_policy()
    }

    // ── Bot identity ──

    /// Get bot identity (cached); also syncs to the pipeline for self-reply filtering.
    pub async fn get_bot_identity(&self) -> Option<Arc<BotIdentity>> {
        let bot = self.bot_identity.get().await;
        if let Some(b) = &bot {
            self.pipeline.set_bot_identity(&b.robot_code);
        }
        bot
    }

    // ── Lifecycle ──

    /// Blocking run of the Stream connection (auto-reconnect, E8).
    /// Returns when closed via [`Self::close`].
    pub async fn start(&self) -> Result<()> {
        if self.core.on_message.read().unwrap().is_none()
            && self.core.on_batch.read().unwrap().is_none()
        {
            return Err(Error::channel("OnMessage handler not registered"));
        }
        if self.cfg_transport() == Transport::Http {
            return Err(Error::channel(
                "http mode has no long-running connection; call handle_http_callback per HTTP request instead of start()",
            ));
        }
        self.conn.run().await
    }

    /// Stop the connection and reconnect loop; flush pending queue batches.
    pub async fn close(&self) {
        self.conn.close();
        self.pipeline.dispose().await;
    }

    fn cfg_transport(&self) -> Transport {
        self.core.cfg.transport
    }

    /// Transport-agnostic inbound message entry, delegated to the safety
    /// pipeline: stale → dedup → self-reply → policy → lock → queue.
    /// Shared by Stream and HTTP modes (Go `processIncoming` parity).
    pub(crate) async fn process_incoming(&self, proto_id: &str, msg: Arc<IncomingMessage>) {
        self.pipeline.push_message(proto_id, msg).await;
    }

    /// Test-only entry mirroring [`Self::process_incoming`] for integration tests.
    #[doc(hidden)]
    pub async fn process_incoming_for_test(&self, proto_id: &str, msg: Arc<IncomingMessage>) {
        self.process_incoming(proto_id, msg).await;
    }

    /// Test-only accessor building a Replier bound to a synthetic message.
    #[doc(hidden)]
    pub fn make_replier_for_test(&self, msg: Arc<IncomingMessage>) -> Arc<dyn crate::reply::Reply> {
        self.core.make_replier(msg) as Arc<dyn crate::reply::Reply>
    }

    // ── Media download ──

    /// Download a media file: exchange downloadCode → URL (SSRF-guarded) → bytes.
    pub async fn download_file(
        &self,
        download_code: impl Into<String>,
        msg_id: impl Into<String>,
    ) -> Result<Vec<u8>> {
        let url = self
            .resolve_media_url(download_code.into(), msg_id.into())
            .await?;
        let resp = self.fetch_media(url).await?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// Stream a media file to a local path without buffering it whole
    /// (aligned with the lark channel-sdk `downloadResourceToFile`).
    ///
    /// SSRF-guarded like [`Channel::download_file`]; the parent directory of
    /// `dest_path` must already exist; the bytes are written to a same-dir
    /// temp file that is atomically renamed on success, so a failure never
    /// leaves a partial file behind. Returns the number of bytes written.
    pub async fn download_file_to_file(
        &self,
        download_code: impl Into<String>,
        msg_id: impl Into<String>,
        dest_path: impl AsRef<std::path::Path>,
    ) -> Result<u64> {
        let dest = dest_path.as_ref();
        let file_name = dest
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::channel("destPath has no valid file name"))?;
        let parent = dest
            .parent()
            .ok_or_else(|| Error::channel("destPath has no parent directory"))?;

        let url = self
            .resolve_media_url(download_code.into(), msg_id.into())
            .await?;
        let resp = self.fetch_media(url).await?;

        let tmp_path = parent.join(format!(
            ".{file_name}.tmp-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        let mut out = match tokio::fs::File::create(&tmp_path).await {
            Ok(f) => f,
            Err(e) => {
                return Err(Error::channel(format!(
                    "cannot create temp file in {}: {e}",
                    parent.display()
                )));
            }
        };

        let mut n: u64 = 0;
        let mut resp = resp;
        let mut write_err: Option<crate::error::Error> = None;
        while let Some(chunk) = match resp.chunk().await {
            Ok(Some(c)) => Some(c),
            Ok(None) => None,
            Err(e) => {
                write_err = Some(Error::channel(format!("download failed: {e}")));
                None
            }
        } {
            if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut out, &chunk).await {
                write_err = Some(Error::channel(format!("write to temp file failed: {e}")));
                break;
            }
            n += chunk.len() as u64;
        }
        drop(out);

        if let Some(e) = write_err {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(e);
        }
        if let Err(e) = tokio::fs::rename(&tmp_path, dest).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(Error::channel(format!("atomic rename failed: {e}")));
        }
        Ok(n)
    }

    /// Exchange a download code for its media URL (SSRF-guarded).
    async fn resolve_media_url(&self, download_code: String, msg_id: String) -> Result<String> {
        if download_code.is_empty() {
            return Err(Error::channel("downloadCode cannot be empty"));
        }
        let path = format!(
            "/v1.0/robot/messageFiles/download?downloadCode={}&messageId={}&robotCode={}",
            urlencoding::encode(&download_code),
            urlencoding::encode(&msg_id),
            urlencoding::encode(&self.core.cfg.client_id),
        );
        let cancel = tokio_util::sync::CancellationToken::new();
        let raw = self
            .core
            .cards
            .call_raw_public(&cancel, reqwest::Method::GET, &path)
            .await?;
        let url = serde_json::from_str::<serde_json::Value>(&raw)?
            .get("downloadUrl")
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        if url.is_empty() {
            return Err(Error::channel("empty download URL"));
        }
        crate::safety::ssrf_guard::assert_public_url_with_allowlist(
            &url,
            &self.core.cfg.ssrf_allowlist,
        )
        .await?;
        Ok(url)
    }

    /// GET a validated media URL and require HTTP 200.
    async fn fetch_media(&self, url: String) -> Result<reqwest::Response> {
        let resp = self.core.http.get(url).send().await?;
        let status = resp.status().as_u16();
        if status != 200 {
            return Err(Error::channel(format!("download failed: http {status}")));
        }
        Ok(resp)
    }
}

/// Route one business frame by topic.
async fn dispatch_frame(core: &Arc<Core>, pipeline: &Arc<SafetyPipeline>, f: &Frame) {
    match f.topic() {
        crate::config::TOPIC_BOT_MESSAGE => handle_bot_message(core, pipeline, f).await,
        crate::config::TOPIC_CARD_INSTANCE_CB => handle_card_action(core, pipeline, f).await,
        other => core
            .cfg
            .debugf(format!("unsubscribed topic {other:?} ignored")),
    }
}

async fn handle_bot_message(core: &Arc<Core>, pipeline: &Arc<SafetyPipeline>, f: &Frame) {
    let msg = match crate::normalize::normalize_incoming(f.data.as_bytes()) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            core.cfg.debugf(format!("bad bot message payload: {e}"));
            return;
        }
    };
    pipeline.push_message(f.message_id(), msg).await;
}

async fn handle_card_action(core: &Arc<Core>, pipeline: &Arc<SafetyPipeline>, f: &Frame) {
    let handler = core.on_card_action.read().unwrap().clone();
    let Some(handler) = handler else { return };

    let d: serde_json::Value = match serde_json::from_str(&f.data) {
        Ok(v) => v,
        Err(e) => {
            core.cfg.debugf(format!("bad card action payload: {e}"));
            return;
        }
    };
    let action = CardAction {
        out_track_id: d
            .get("outTrackId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        user_id: d
            .get("userId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        data_content: d
            .get("dataContent")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        raw: d,
    };

    // Card callbacks go through PushAction: dedup (delivery ID + action content
    // fingerprint, replay defense) + processing lock + same-card serialization.
    let action_fp = crate::safety::content_fingerprint(
        &format!("card:{}", action.out_track_id),
        0,
        &action.user_id,
        &action.data_content.to_string(),
    );
    let extra_keys = vec![action_fp];

    let core2 = core.clone();
    let action2 = action.clone();
    let task: BoxFuture<'static, Result<()>> = Box::pin(async move {
        let reply = core2.make_replier(Arc::new(IncomingMessage {
            conversation_id: action2.out_track_id.clone(),
            session_webhook: String::new(),
            ..Default::default()
        }));
        handler(action2, reply as Arc<dyn crate::reply::Reply>).await
    });

    let scope = format!("card:{}", action.out_track_id);
    pipeline
        .push_action(f.message_id(), &scope, &extra_keys, task)
        .await;
}
