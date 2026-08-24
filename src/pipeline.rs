//! Unified safety pipeline facade (Go `safety/pipeline.go` port).
//!
//! Three-tier push interface:
//! - [`SafetyPipeline::push_message`]: full pipeline (stale → dedup → self-sent
//!   → policy → processing lock → serial/batch queue)
//! - [`SafetyPipeline::push_action`][]: simplified (dedup → lock → scoped serial)
//!   for card callbacks
//! - [`SafetyPipeline::push_light`]: dedup only, for lightweight events

use crate::safety::chat_queue::{ChatQueueManager, FlushHandler};
use crate::safety::{content_fingerprint, PolicyGate, ProcessingLock, SeenCache, StaleDetector};
use crate::types::{BatchedMessage, IncomingMessage, RejectEvent, RejectReason, SafetyConfig};
use futures_util::future::BoxFuture;
use std::sync::Arc;

/// Message dispatch callback; `sources` are the originals of a merged batch.
pub type MessageDispatchFn = Arc<
    dyn Fn(
            Arc<IncomingMessage>,
            Vec<Arc<IncomingMessage>>,
        ) -> BoxFuture<'static, crate::error::Result<()>>
        + Send
        + Sync,
>;

/// Batch dispatch callback (OnBatch path).
pub type BatchDispatchFn =
    Arc<dyn Fn(BatchedMessage) -> BoxFuture<'static, crate::error::Result<()>> + Send + Sync>;

pub type RejectFn = Arc<dyn Fn(RejectEvent) + Send + Sync>;

pub(crate) struct PipelineOptions {
    pub on_message: MessageDispatchFn,
    pub on_batch: Option<BatchDispatchFn>,
    /// Dynamic probe: is OnBatch registered? Decides batch vs serial path.
    pub has_on_batch: Box<dyn Fn() -> bool + Send + Sync>,
    pub on_reject: Option<RejectFn>,
    pub chat_queue: Arc<ChatQueueManager>,
    pub bot_robot_code: String,
}

pub(crate) struct SafetyPipeline {
    cfg: SafetyConfig,
    stale: StaleDetector,
    seen: Arc<SeenCache>,
    lock: Arc<ProcessingLock>,
    policy: Arc<PolicyGate>,
    chat_queue: Arc<ChatQueueManager>,
    opts: PipelineOptions,
}

impl SafetyPipeline {
    pub(crate) fn new(cfg: SafetyConfig, opts: PipelineOptions) -> Self {
        let stale_window = if cfg.stale_window.is_zero() {
            std::time::Duration::from_secs(30 * 60)
        } else {
            cfg.stale_window
        };
        let lock_ttl = if cfg.lock_ttl.is_zero() {
            std::time::Duration::from_secs(5 * 60)
        } else {
            cfg.lock_ttl
        };
        Self {
            policy: Arc::new(PolicyGate::new(cfg.policy.clone())),
            seen: Arc::new(SeenCache::new(cfg.dedup.clone())),
            lock: Arc::new(ProcessingLock::new(lock_ttl)),
            stale: StaleDetector::new(stale_window),
            chat_queue: opts.chat_queue.clone(),
            cfg,
            opts,
        }
    }

    fn batch_mode(&self) -> bool {
        self.opts.on_batch.is_some() && (self.opts.has_on_batch)()
    }

    /// Push an inbound message through the full pipeline.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn push_message(&self, proto_id: &str, msg: Arc<IncomingMessage>) {
        // 1. Stale detection.
        if self.stale.is_stale(msg.create_at) {
            self.emit_reject(&msg, RejectReason::Stale);
            return;
        }

        // 2. Dedup: protocol delivery ID + business msgId; when timestamp and
        //    content exist also add a content fingerprint (gateway ID-rotation
        //    replay defense). createAt==0 → fingerprint skipped (no signal).
        let mut keys: Vec<String> = vec![proto_id.to_string(), msg.msg_id.clone()];
        if !msg.text.is_empty() && msg.create_at > 0 {
            keys.push(content_fingerprint(
                &msg.conversation_id,
                msg.create_at,
                &msg.msg_type,
                &msg.text,
            ));
        }
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let dup = if self.cfg.mark_after_handler {
            self.seen.has(&key_refs)
        } else {
            self.seen.check_and_mark(&key_refs)
        };
        if dup {
            self.emit_reject(&msg, RejectReason::Duplicate);
            return;
        }

        // 3. Self-reply filter (only when bot identity is known).
        if self.cfg.drop_self_sent
            && !self.opts.bot_robot_code.is_empty()
            && msg.sender_id == self.opts.bot_robot_code
        {
            self.emit_reject(&msg, RejectReason::SelfSent);
            return;
        }

        // 4. Policy gate.
        let decision = self.policy.evaluate(&msg);
        if !decision.allowed {
            self.emit_reject(&msg, decision.reason);
            return;
        }

        // 5. Processing lock: prevent concurrent handling of one message.
        if !self.lock.acquire(&msg.msg_id) {
            self.emit_reject(&msg, RejectReason::LockContention);
            return;
        }

        // 6. Queue path: per-chat serial (OnMessage) or batching (OnBatch).
        if self.chat_queue.enabled() {
            if self.batch_mode() {
                let scope = msg.conversation_id.clone();
                let on_batch = self.opts.on_batch.clone().expect("batch mode checked");
                let seen = self.seen.clone();
                let mark_after = self.cfg.mark_after_handler;
                let lock = self.lock.clone();
                let handler: FlushHandler = Arc::new(move |d: BatchedMessage| {
                    let on_batch = on_batch.clone();
                    let seen = seen.clone();
                    let lock = lock.clone();
                    Box::pin(async move {
                        let res = (on_batch)(d.clone()).await;
                        for id in &d.source_ids {
                            lock.release(id);
                        }
                        if mark_after && res.is_ok() {
                            let refs: Vec<&str> = d.source_ids.iter().map(|s| s.as_str()).collect();
                            seen.add(&refs);
                        }
                    })
                });
                self.chat_queue.push(&scope, msg, handler).await;
                return;
            }
            let scope = msg.conversation_id.clone();
            let on_message = self.opts.on_message.clone();
            let seen = self.seen.clone();
            let lock = self.lock.clone();
            let mark_after = self.cfg.mark_after_handler;
            let m = msg.clone();
            let task = Box::pin(async move {
                let res = (on_message)(m.clone(), vec![m.clone()]).await;
                lock.release(&m.msg_id);
                if mark_after && res.is_ok() {
                    seen.add(&[m.msg_id.as_str()]);
                }
                res
            });
            let _ = self.chat_queue.run_serial(&scope, task).await;
            return;
        }

        // 7. Direct dispatch.
        let on_message = self.opts.on_message.clone();
        let seen = self.seen.clone();
        let lock = self.lock.clone();
        let mark_after = self.cfg.mark_after_handler;
        let m = msg.clone();
        let task = Box::pin(async move {
            let res = (on_message)(m.clone(), vec![m.clone()]).await;
            lock.release(&m.msg_id);
            if mark_after && res.is_ok() {
                seen.add(&[m.msg_id.as_str()]);
            }
            res
        });
        let _ = task.await;
    }

    /// Push an action event (card callback) through the simplified pipeline:
    /// dedup → lock → scoped serial execution. Extra dedup keys defend against
    /// gateway ID-rotation replay of the same action.
    pub(crate) async fn push_action(
        &self,
        event_id: &str,
        scope: &str,
        extra_dedup_keys: &[String],
        handler: BoxFuture<'static, crate::error::Result<()>>,
    ) {
        let mut keys: Vec<String> = vec![event_id.to_string()];
        keys.extend(extra_dedup_keys.iter().cloned());
        let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

        let dup = if self.cfg.mark_after_handler {
            self.seen.has(&refs)
        } else {
            self.seen.check_and_mark(&refs)
        };
        if dup {
            return;
        }
        if !self.lock.acquire(event_id) {
            return;
        }

        let seen = self.seen.clone();
        let lock = self.lock.clone();
        let event_id_owned = event_id.to_string();
        let mark_after = self.cfg.mark_after_handler;
        let runner = Box::pin(async move {
            let err = handler.await;
            lock.release(&event_id_owned);
            if mark_after && err.is_ok() {
                seen.add(&[event_id_owned.as_str()]);
            }
            err
        });

        if self.chat_queue.enabled() {
            let _ = self.chat_queue.run_serial(scope, runner).await;
        } else {
            let _ = runner.await;
        }
    }

    /// Push a lightweight event (reaction etc.): dedup only, then execute.
    #[allow(dead_code)]
    pub(crate) async fn push_light(
        &self,
        event_id: &str,
        handler: BoxFuture<'static, crate::error::Result<()>>,
    ) {
        let refs = [event_id];
        let dup = if self.cfg.mark_after_handler {
            self.seen.has(&refs)
        } else {
            self.seen.check_and_mark(&refs)
        };
        if dup {
            return;
        }
        let res = handler.await;
        if self.cfg.mark_after_handler && res.is_ok() {
            self.seen.add(&[event_id]);
        }
    }

    fn emit_reject(&self, msg: &IncomingMessage, reason: RejectReason) {
        if let Some(on_reject) = &self.opts.on_reject {
            on_reject(RejectEvent {
                message_id: msg.msg_id.clone(),
                chat_id: msg.conversation_id.clone(),
                sender_id: msg.sender_id.clone(),
                reason,
            });
        }
    }

    pub(crate) fn set_bot_identity(&self, robot_code: &str) {
        self.policy.set_bot_identity(crate::types::BotIdentity {
            robot_code: robot_code.to_string(),
            ..Default::default()
        });
    }

    pub(crate) fn update_policy(&self, cfg: crate::types::PolicyConfig) {
        self.policy.update_config(cfg);
    }

    pub(crate) fn get_policy(&self) -> crate::types::PolicyConfig {
        self.policy.get_config()
    }

    pub(crate) async fn dispose(&self) {
        self.chat_queue.flush_all().await;
        self.seen.clear();
    }
}
