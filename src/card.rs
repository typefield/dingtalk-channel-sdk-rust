//! AI-card five-step protocol client + streaming handle (SPEC §5; Go `card.go` port).
//!
//! Steps: create → deliver → INPUTING first frame → streaming updates → FINISHED.
//! Global token-bucket rate limiting with QpsLimit backoff (SPEC §6).

use crate::config::Config;
use crate::error::{ApiError, Error, Result};
use crate::ratelimit::TokenBucket;
use crate::token::TokenProvider;
use rand::Rng;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

// flowStatus (SPEC §5)
#[allow(dead_code)]
pub(crate) const FLOW_PROCESSING: &str = "1";
pub(crate) const FLOW_INPUTING: &str = "2";
pub(crate) const FLOW_FINISHED: &str = "3";
pub(crate) const FLOW_FAILED: &str = "5";

/// Frame gap (dws-proven): back-to-back frames race the client's card fetch
/// and intermittently render "内容加载失败".
pub(crate) const CARD_FRAME_GAP: Duration = Duration::from_millis(500);

/// Per-frame content cap (rune-safe truncation; hermes MAX_MESSAGE_LENGTH parity).
pub(crate) const CARD_MAX_CONTENT: usize = 20000;

const RAND_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

pub(crate) fn rand_suffix(n: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..n)
        .map(|_| RAND_ALPHABET[rng.gen_range(0..RAND_ALPHABET.len())] as char)
        .collect()
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Card API client.
pub(crate) struct CardClient {
    pub cfg: Arc<Config>,
    tokens: Arc<TokenProvider>,
    bucket: Arc<TokenBucket>,
    http: reqwest::Client,
}

#[derive(Debug, Clone)]
pub(crate) struct CardTarget {
    pub is_group: bool,
    pub conversation_id: String,
    pub user_id: String,
    pub robot_code: String,
}

/// One delivered AI card instance.
#[derive(Debug, Clone)]
pub(crate) struct CardInstance {
    pub out_track_id: String,
    pub inputing_started: bool,
}

impl CardClient {
    pub(crate) fn new(
        cfg: Arc<Config>,
        tokens: Arc<TokenProvider>,
        bucket: Arc<TokenBucket>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            cfg,
            tokens,
            bucket,
            http,
        }
    }

    /// Raw call with global rate limit + one-shot QpsLimit retry.
    pub(crate) async fn call_raw(
        &self,
        cancel: &CancellationToken,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<String> {
        let payload = body.map(|b| b.to_string());

        let do_once = || {
            let url = format!("{}{}", self.cfg.api_base, path);
            let payload = payload.clone();
            let method = method.clone();
            let http = self.http.clone();
            let tokens = self.tokens.clone();
            let cancel = cancel.clone();
            async move {
                if cancel.is_cancelled() {
                    return Err(Error::channel("context canceled"));
                }
                let token = tokens.get().await?;
                let mut req = http
                    .request(method, &url)
                    .header("Content-Type", "application/json")
                    .header("x-acs-dingtalk-access-token", token);
                if let Some(p) = payload {
                    req = req.body(p);
                }
                let resp = req.send().await?;
                let status = resp.status().as_u16();
                let raw = resp.text().await?;
                if status >= 400 {
                    let code = serde_json::from_str::<Value>(&raw)
                        .ok()
                        .and_then(|v| {
                            v.get("code")
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string())
                        })
                        .unwrap_or_default();
                    let msg = serde_json::from_str::<Value>(&raw)
                        .ok()
                        .and_then(|v| {
                            v.get("message")
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string())
                        })
                        .unwrap_or_default();
                    return Err(Error::Api(ApiError {
                        status,
                        code,
                        msg,
                        body: raw,
                    }));
                }
                Ok(raw)
            }
        };

        // Global rate limit: take a token before every call.
        self.bucket.wait_for(cancel).await?;

        match do_once().await {
            Ok(raw) => Ok(raw),
            Err(e) => {
                if let Error::Api(api) = &e {
                    if api.is_qps_limit() {
                        self.bucket.trigger_backoff();
                        self.bucket.wait_for(cancel).await?;
                        return do_once().await;
                    }
                }
                Err(e)
            }
        }
    }

    pub(crate) async fn call(
        &self,
        cancel: &CancellationToken,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<()> {
        self.call_raw(cancel, method, path, body).await.map(|_| ())
    }

    /// Business-level validation inside HTTP 200: card APIs return
    /// `{"result":[{"success":false,...}]}` in a 200 body (dws production evidence),
    /// which must be treated as failure.
    async fn call_checked(
        &self,
        cancel: &CancellationToken,
        method: reqwest::Method,
        path: &str,
        body: &Value,
    ) -> Result<()> {
        let raw = self.call_raw(cancel, method, path, Some(body)).await?;
        if raw.contains(r#""success":false"#) {
            return Err(Error::Api(ApiError {
                status: 200,
                code: "BusinessFailure".into(),
                msg: raw.clone(),
                body: raw,
            }));
        }
        Ok(())
    }

    /// Create + deliver a card for the target (steps 1–2).
    pub(crate) async fn create_and_deliver(
        &self,
        cancel: &CancellationToken,
        t: &CardTarget,
    ) -> Result<CardInstance> {
        let out_track_id = format!("card_{}_{}", now_millis(), rand_suffix(8));

        let create_body = json!({
            "cardTemplateId": self.cfg.card_template_id,
            "outTrackId": out_track_id,
            "cardData": {"cardParamMap": {"config": r#"{"autoLayout":true}"#}},
            "callbackType": "STREAM",
            "imGroupOpenSpaceModel": {"supportForward": true},
            "imRobotOpenSpaceModel": {"supportForward": true},
        });
        self.call(
            cancel,
            reqwest::Method::POST,
            "/v1.0/card/instances",
            Some(&create_body),
        )
        .await?;

        let deliver_body = if t.is_group {
            json!({
                "outTrackId": out_track_id,
                "userIdType": 1,
                "openSpaceId": format!("dtv1.card//IM_GROUP.{}", t.conversation_id),
                "imGroupOpenDeliverModel": {"robotCode": t.robot_code},
            })
        } else {
            json!({
                "outTrackId": out_track_id,
                "userIdType": 1,
                "openSpaceId": format!("dtv1.card//IM_ROBOT.{}", t.user_id),
                "imRobotOpenDeliverModel": {
                    "spaceType": "IM_ROBOT",
                    "robotCode": t.robot_code,
                    "extension": {"dynamicSummary": "true"},
                },
            })
        };
        self.call_checked(
            cancel,
            reqwest::Method::POST,
            "/v1.0/card/instances/deliver",
            &deliver_body,
        )
        .await?;

        Ok(CardInstance {
            out_track_id,
            inputing_started: false,
        })
    }

    /// Update flowStatus/msgContent (INPUTING first frame / FINISHED close-out).
    pub(crate) async fn set_status(
        &self,
        cancel: &CancellationToken,
        card: &CardInstance,
        status: &str,
        content: &str,
    ) -> Result<()> {
        let mut body = json!({
            "outTrackId": card.out_track_id,
            "cardData": {"cardParamMap": {
                "flowStatus": status,
                "msgContent": content,
                "staticMsgContent": "",
                "sys_full_json_obj": r#"{"order":["msgContent"]}"#,
                "config": r#"{"autoLayout":true}"#,
            }},
        });
        if status == FLOW_FINISHED {
            body["cardUpdateOptions"] = json!({"updateCardDataByKey": true});
        }
        self.call(
            cancel,
            reqwest::Method::PUT,
            "/v1.0/card/instances",
            Some(&body),
        )
        .await
    }

    /// Streaming update frame; `finalize=true` marks the final frame.
    pub(crate) async fn stream_frame(
        &self,
        cancel: &CancellationToken,
        card: &CardInstance,
        content: &str,
        finalize: bool,
    ) -> Result<()> {
        let mut norm = crate::outbound::normalize_for_card(content);
        if !finalize {
            norm = norm.trim_end_matches('\n').to_string();
        }
        let body = json!({
            "outTrackId": card.out_track_id,
            "guid": format!("{}_{}", now_millis(), rand_suffix(6)),
            "key": "msgContent",
            "content": norm,
            "isFull": true,
            "isFinalize": finalize,
            "isError": false,
        });
        self.call(
            cancel,
            reqwest::Method::PUT,
            "/v1.0/card/streaming",
            Some(&body),
        )
        .await
    }
}

/// Trailing-newline strip for non-final frames (anti-flicker).
#[allow(dead_code)]
pub(crate) fn strip_trailing_newlines(s: &str) -> String {
    s.trim_end_matches('\n').to_string()
}

struct StreamerState {
    accumulated: String,
    last_update: Instant,
    closed: bool,
    has_pending: bool,
    frame_count: usize,
    last_frame_at: Instant,
    aborted: bool,
    last_activity: Instant,
}

pub(crate) type FallbackFn =
    Arc<dyn Fn(String) -> futures_util::future::BoxFuture<'static, Result<()>> + Send + Sync>;
pub(crate) type DeliverRestFn =
    Arc<dyn Fn(String) -> futures_util::future::BoxFuture<'static, ()> + Send + Sync>;

/// Streaming card handle: throttled append, finish/fail/abort semantics (E1–E4).
///
/// Flush-controller effects:
/// - trailing flush: updates inside the throttle window are never dropped
/// - long-gap batching: after >2s without updates the first flush waits 300ms
///   so the first visible update carries meaningful text
pub struct CardStreamer {
    inner: Arc<StreamerInner>,
}

struct StreamerInner {
    state: parking_lot_lite::Mutex<StreamerState>,
    client: Arc<CardClient>,
    card: parking_lot_lite::Mutex<Option<CardInstance>>,
    throttle: Duration,
    watchdog: Duration,
    cancel: CancellationToken,
    fallback: Option<FallbackFn>,
    deliver_rest: Option<DeliverRestFn>,
}

mod parking_lot_lite {
    /// Tiny wrapper to keep imports minimal while giving us a plain Mutex.
    pub type Mutex<T> = std::sync::Mutex<T>;
}

const LONG_GAP_THRESHOLD: Duration = Duration::from_secs(2);
const LONG_GAP_BATCH_WAIT: Duration = Duration::from_millis(300);

impl CardStreamer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: Arc<CardClient>,
        card: Option<CardInstance>,
        throttle: Duration,
        watchdog: Duration,
        cancel: CancellationToken,
        fallback: Option<FallbackFn>,
        deliver_rest: Option<DeliverRestFn>,
    ) -> Self {
        let now = Instant::now();
        let inner = Arc::new(StreamerInner {
            state: parking_lot_lite::Mutex::new(StreamerState {
                accumulated: String::new(),
                last_update: now,
                closed: false,
                has_pending: false,
                frame_count: 0,
                last_frame_at: now,
                aborted: false,
                last_activity: now,
            }),
            client,
            card: parking_lot_lite::Mutex::new(card),
            throttle,
            watchdog,
            cancel,
            fallback,
            deliver_rest,
        });
        let s = Self { inner };
        s.arm_watchdog();
        s
    }

    /// Whether the card was really created and delivered (false = degraded mode).
    pub fn card_delivered(&self) -> bool {
        self.inner.card.lock().unwrap().is_some()
    }

    /// Append a delta. Accumulation semantics are caller-defined.
    pub async fn append(&self, delta: impl Into<String>) -> Result<()> {
        let delta = delta.into();
        let (update_now, content) = {
            let mut st = self.inner.state.lock().unwrap();
            if st.closed {
                return Err(Error::channel("streamer already closed"));
            }
            st.accumulated.push_str(&delta);
            if self.inner.card.lock().unwrap().is_none() {
                return Ok(()); // degraded mode: accumulate only, Finish falls back
            }
            let now = Instant::now();
            let elapsed = now.duration_since(st.last_update);
            if elapsed >= self.inner.throttle && elapsed > LONG_GAP_THRESHOLD {
                // Long gap (tool calls / thinking): delay-batch the first visible update.
                schedule_pending_locked(&self.inner, &mut st, LONG_GAP_BATCH_WAIT);
                (false, String::new())
            } else if elapsed >= self.inner.throttle {
                st.last_update = now;
                (true, st.accumulated.clone())
            } else {
                if !st.has_pending {
                    schedule_pending_locked(&self.inner, &mut st, self.inner.throttle - elapsed);
                }
                (false, String::new())
            }
        };
        if update_now {
            update_streamer(&self.inner, &content, false).await?;
        }
        Ok(())
    }

    /// Finalize: final frame + FINISHED status. Non-empty `text` overrides accumulation.
    pub async fn finish(&self, text: impl Into<String>) -> Result<()> {
        let text = text.into();
        let (content, card, needs_content_frame) = {
            let mut st = self.inner.state.lock().unwrap();
            if st.closed {
                return Ok(()); // idempotent
            }
            st.closed = true;
            if !text.is_empty() {
                st.accumulated = text;
            }
            (
                st.accumulated.clone(),
                self.inner.card.lock().unwrap().clone(),
                st.frame_count == 0 && !st.accumulated.is_empty(),
            )
        };

        let Some(card) = card else {
            return self.try_fallback(&content).await;
        };
        // A quick response can finish before the throttled trailing flush runs.
        // Preserve the streaming contract by emitting one content frame before
        // the final frame instead of letting `closed` discard the pending flush.
        if needs_content_frame {
            update_streamer(&self.inner, &content, false).await?;
        }
        if let Err(e) = update_streamer(&self.inner, &content, true).await {
            let _ = self.try_fallback(&content).await; // E4: degrade so users still get the reply
            return Err(e);
        }
        let err = self
            .inner
            .client
            .set_status(
                &self.inner.cancel,
                &card,
                FLOW_FINISHED,
                &crate::outbound::normalize_for_card(&content),
            )
            .await;
        if err.is_err() {
            let _ = self.try_fallback(&content).await;
        }
        // Overflow beyond single-frame cap continues via webhook chunks (best-effort).
        if let Some(rest) = overflow_remainder(&content) {
            if let Some(deliver) = &self.inner.deliver_rest {
                deliver(rest).await;
            }
        }
        self.inner.cancel.cancel();
        err
    }

    /// Explicit abort: seal the stream and mark the card FAILED.
    /// Mutually exclusive with Finish/Fail and idempotent.
    pub async fn abort(&self) -> Result<()> {
        let (closed_before, card, content) = {
            let mut st = self.inner.state.lock().unwrap();
            let already = st.closed;
            st.closed = true;
            st.aborted = true;
            (
                already,
                self.inner.card.lock().unwrap().clone(),
                st.accumulated.clone(),
            )
        };
        if closed_before {
            return Ok(());
        }
        let Some(card) = card else { return Ok(()) };
        let _ = self
            .inner
            .client
            .stream_frame(&self.inner.cancel, &card, &content, true)
            .await;
        self.inner
            .client
            .set_status(
                &self.inner.cancel,
                &card,
                FLOW_FAILED,
                &crate::outbound::normalize_for_card(&content),
            )
            .await
    }

    /// Mark FAILED and send the error text through the fallback channel.
    pub async fn fail(&self, err_text: impl Into<String>) -> Result<()> {
        let err_text = err_text.into();
        {
            let mut st = self.inner.state.lock().unwrap();
            if st.closed {
                return Ok(());
            }
            st.closed = true;
        }
        let card = self.inner.card.lock().unwrap().clone();
        if let Some(card) = card {
            let inputing_needed = !card.inputing_started;
            if inputing_needed {
                let _ = self
                    .inner
                    .client
                    .set_status(&self.inner.cancel, &card, FLOW_INPUTING, "")
                    .await;
                if let Some(c) = self.inner.card.lock().unwrap().as_mut() {
                    c.inputing_started = true;
                }
            }
            let _ = self
                .inner
                .client
                .stream_frame(&self.inner.cancel, &card, &err_text, true)
                .await;
            let _ = self
                .inner
                .client
                .set_status(
                    &self.inner.cancel,
                    &card,
                    FLOW_FAILED,
                    &crate::outbound::normalize_for_card(&err_text),
                )
                .await;
        }
        let fb = self.try_fallback(&err_text).await;
        self.inner.cancel.cancel();
        fb
    }

    async fn try_fallback(&self, text: &str) -> Result<()> {
        if let Some(fallback) = &self.inner.fallback {
            if text.trim().is_empty() {
                return Ok(());
            }
            // 10s timeout like Go.
            let fut = fallback(text.to_string());
            match tokio::time::timeout(Duration::from_secs(10), fut).await {
                Ok(res) => res,
                Err(_) => Err(Error::channel("fallback timeout")),
            }
        } else {
            Ok(())
        }
    }

    fn arm_watchdog(&self) {
        if self.inner.watchdog.is_zero() {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let watchdog = self.inner.watchdog;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            loop {
                tick.tick().await;
                let Some(inner) = weak.upgrade() else { return };
                let force = {
                    let mut st = inner.state.lock().unwrap();
                    if st.closed || inner.card.lock().unwrap().is_none() {
                        false
                    } else if st.last_activity.elapsed() > watchdog {
                        st.closed = true; // seal: late callbacks must not create frames
                        true
                    } else {
                        false
                    }
                };
                if force {
                    let content = inner.state.lock().unwrap().accumulated.clone();
                    let card = inner.card.lock().unwrap().clone();
                    if let Some(card) = card {
                        let _ = inner
                            .client
                            .stream_frame(&inner.cancel, &card, &content, true)
                            .await;
                        let _ = inner
                            .client
                            .set_status(
                                &inner.cancel,
                                &card,
                                FLOW_FINISHED,
                                &crate::outbound::normalize_for_card(&content),
                            )
                            .await;
                    }
                    return;
                }
            }
        });
    }
}

/// Caller must hold the state lock; spawns the trailing-flush task.
fn schedule_pending_locked(inner: &Arc<StreamerInner>, st: &mut StreamerState, delay: Duration) {
    if st.has_pending {
        return;
    }
    st.has_pending = true;
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let Some(inner) = weak.upgrade() else { return };
        let content = {
            let mut st = inner.state.lock().unwrap();
            st.has_pending = false;
            if st.closed || inner.card.lock().unwrap().is_none() {
                return;
            }
            st.last_update = Instant::now();
            st.accumulated.clone()
        };
        let _ = update_streamer(&inner, &content, false).await;
    });
}

async fn update_streamer(inner: &Arc<StreamerInner>, content: &str, finalize: bool) -> Result<()> {
    let card = inner.card.lock().unwrap().clone();
    let Some(card) = card else {
        return Err(Error::channel("card unavailable"));
    };

    // Frame gap before first content frame and before the final frame
    // (prevents the "内容加载失败" render race).
    let need_gap = {
        let st = inner.state.lock().unwrap();
        st.frame_count == 0 || finalize
    };
    if need_gap {
        let elapsed = Instant::now().duration_since(inner.state.lock().unwrap().last_frame_at);
        if elapsed < CARD_FRAME_GAP {
            tokio::select! {
                _ = tokio::time::sleep(CARD_FRAME_GAP - elapsed) => {}
                _ = inner.cancel.cancelled() => return Err(Error::channel("context canceled")),
            }
        }
    }

    // Rune-safe truncation to the per-frame cap.
    let content_owned: String = if content.chars().count() > CARD_MAX_CONTENT {
        content.chars().take(CARD_MAX_CONTENT).collect()
    } else {
        content.to_string()
    };

    // First content frame must be preceded by an INPUTING status update.
    if !card.inputing_started {
        inner
            .client
            .set_status(
                &inner.cancel,
                &card,
                FLOW_INPUTING,
                &crate::outbound::normalize_for_card(&content_owned),
            )
            .await?;
        if let Some(c) = inner.card.lock().unwrap().as_mut() {
            c.inputing_started = true;
        }
    }
    {
        let mut st = inner.state.lock().unwrap();
        st.frame_count += 1;
        st.last_frame_at = Instant::now();
        st.last_activity = st.last_frame_at;
    }
    inner
        .client
        .stream_frame(&inner.cancel, &card, &content_owned, finalize)
        .await
}

/// Tail content beyond the single-frame cap (empty when within limits).
pub(crate) fn overflow_remainder(content: &str) -> Option<String> {
    let total = content.chars().count();
    if total > CARD_MAX_CONTENT {
        Some(content.chars().skip(CARD_MAX_CONTENT).collect())
    } else {
        None
    }
}
