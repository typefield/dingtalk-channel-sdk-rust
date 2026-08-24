//! Stream long connection: gateway open → WebSocket → read loop with
//! heartbeat, ACK-first dispatch and exponential-backoff reconnect
//! (SPEC §2; Go `stream.go` port).

use crate::config::{Config, DEFAULT_PONG_WAIT, DEFAULT_RECONNECT_BASE, DEFAULT_RECONNECT_MAX};
use crate::error::{Error, Result};
use crate::frame::{
    success_ack, Frame, OpenConnectionRequest, OpenConnectionResponse, Subscription, SUB_CALLBACK,
    SUB_SYSTEM,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;

/// Callback invoked for each business frame; return value becomes the ACK data
/// (empty → `{"success":true}`).
pub(crate) type OnFrameFn =
    Arc<dyn Fn(Frame) -> futures_util::future::BoxFuture<'static, String> + Send + Sync>;

pub(crate) struct StreamConn {
    cfg: Arc<Config>,
    http: reqwest::Client,
    on_frame: OnFrameFn,
    /// Extra-subscribe the card callback topic when a card handler is registered.
    card_topic_wanted: std::sync::atomic::AtomicBool,
    hooks: Arc<crate::lifecycle::LifecycleHooks>,
    stop: CancellationToken,
}

impl StreamConn {
    pub(crate) fn new(
        cfg: Arc<Config>,
        http: reqwest::Client,
        on_frame: OnFrameFn,
        hooks: Arc<crate::lifecycle::LifecycleHooks>,
    ) -> Self {
        Self {
            cfg,
            http,
            on_frame,
            card_topic_wanted: Default::default(),
            hooks,
            stop: CancellationToken::new(),
        }
    }

    pub(crate) fn set_card_topic_wanted(&self, wanted: bool) {
        self.card_topic_wanted
            .store(wanted, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn close(&self) {
        self.stop.cancel();
    }

    #[allow(dead_code)]
    pub(crate) fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }

    async fn open(&self) -> Result<(String, String)> {
        let mut subs = vec![
            Subscription {
                r#type: SUB_SYSTEM.into(),
                topic: "ping".into(),
            },
            Subscription {
                r#type: SUB_SYSTEM.into(),
                topic: "disconnect".into(),
            },
            Subscription {
                r#type: SUB_CALLBACK.into(),
                topic: crate::config::TOPIC_BOT_MESSAGE.into(),
            },
        ];
        if self
            .card_topic_wanted
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            subs.push(Subscription {
                r#type: SUB_CALLBACK.into(),
                topic: crate::config::TOPIC_CARD_INSTANCE_CB.into(),
            });
        }
        let req = OpenConnectionRequest {
            client_id: self.cfg.client_id.clone(),
            client_secret: self.cfg.client_secret.clone(),
            subscriptions: subs,
            ua: crate::config::USER_AGENT.into(),
            local_ip: local_lan_ip(),
        };
        let url = format!("{}/v1.0/gateway/connections/open", self.cfg.api_base);
        let resp = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("User-Agent", crate::config::USER_AGENT)
            .json(&req)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let raw = resp.text().await?;
        if status != 200 {
            return Err(Error::channel(format!(
                "gateway open: http {status}: {raw}"
            )));
        }
        let out: OpenConnectionResponse = serde_json::from_str(&raw)?;
        if out.endpoint.is_empty() || out.ticket.is_empty() {
            return Err(Error::channel("gateway open: empty endpoint/ticket"));
        }
        Ok((out.endpoint, out.ticket))
    }

    /// Blocking run loop: connect → read; auto-reconnect with backoff (E8).
    pub(crate) async fn run(&self) -> Result<()> {
        let mut attempt: u32 = 0;
        let mut first_connect = true;
        loop {
            let (connected, err) = self.run_once(first_connect).await;
            if connected {
                // A connection was established this round: reset backoff so the
                // next reconnect starts from the minimum interval.
                attempt = 0;
            }
            first_connect = false;
            if self.stop.is_cancelled() {
                return Ok(());
            }
            if !self.cfg.auto_reconnect {
                return Err(err.unwrap_or_else(|| Error::channel("stream closed")));
            }
            self.hooks.fire_disconnected();
            self.hooks.fire_reconnecting();
            let delay = backoff_delay(attempt);
            attempt += 1;
            if let Some(e) = &err {
                self.cfg
                    .debugf(format!("stream disconnected ({e}), reconnect in {delay:?}"));
            } else {
                self.cfg
                    .debugf(format!("stream disconnected, reconnect in {delay:?}"));
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.stop.cancelled() => return Ok(()),
            }
        }
    }

    async fn run_once(&self, first_connect: bool) -> (bool, Option<Error>) {
        // Dial budget (gateway open + ws handshake) like Go's 10s dial timeout.
        let dial = tokio::time::timeout(Duration::from_secs(10), async {
            let (endpoint, ticket) = self.open().await?;
            // ticket must be URL-encoded (official Python SDK parity);
            // the topic is declared in the open request, not the URL.
            let wss_url = format!("{}?ticket={}", endpoint, urlencoding::encode(&ticket));
            let (ws, _resp) = tokio_tungstenite::connect_async(&wss_url).await?;
            Ok::<_, Error>(ws)
        })
        .await;

        let ws = match dial {
            Err(_) => {
                let e = Error::channel("dial timeout");
                self.hooks.fire_error(&e);
                return (false, Some(e));
            }
            Ok(Err(e)) => {
                self.hooks.fire_error(&e);
                return (false, Some(e));
            }
            Ok(Ok(ws)) => ws,
        };

        self.cfg.debugf("stream connected");
        if first_connect {
            self.hooks.fire_ready();
        } else {
            self.hooks.fire_reconnected();
        }

        let result = self.read_loop(ws).await;
        match result {
            Ok(()) => (true, None),
            Err(e) => {
                self.hooks.fire_error(&e);
                (true, Some(e))
            }
        }
    }

    async fn read_loop(
        &self,
        ws: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<()> {
        let (mut sink, mut stream) = ws.split();
        let pong_at = Arc::new(std::sync::atomic::AtomicI64::new(0));

        // Writer task: serializes outbound frames/acks/pings from channels.
        enum Outbound {
            Text(String),
            Ping,
        }
        let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Outbound>();
        let writer_stop = self.stop.clone();
        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                let res = match msg {
                    Outbound::Text(t) => sink.send(WsMessage::text(t)).await,
                    Outbound::Ping => sink.send(WsMessage::Ping(Default::default())).await,
                };
                if res.is_err() {
                    break;
                }
                if writer_stop.is_cancelled() {
                    break;
                }
            }
        });

        let idle = self.cfg.keep_alive_idle;
        let result = loop {
            let recv_next = tokio::time::timeout(idle, stream.next());
            tokio::select! {
                _ = self.stop.cancelled() => break Ok(()),
                item = recv_next => {
                    match item {
                        Err(_elapsed) => {
                            // Idle window elapsed: send protocol-level ping, wait 5s for pong.
                            pong_at.store(0, std::sync::atomic::Ordering::SeqCst);
                            if out_tx.send(Outbound::Ping).is_err() {
                                break Err(Error::channel("writer closed"));
                            }
                            let waited = tokio::time::timeout(DEFAULT_PONG_WAIT, async {
                                while pong_at.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                            })
                            .await;
                            if waited.is_err() {
                                break Err(Error::channel("pong timeout"));
                            }
                        }
                        Ok(None) => break Err(Error::channel("connection closed")),
                        Ok(Some(Err(e))) => break Err(Error::from(e)),
                        Ok(Some(Ok(msg))) => {
                            match msg {
                                WsMessage::Text(text) => {
                                    let handled = self.handle_frame(&text);
                                    if let Some(ack) = handled.ack {
                                        let _ = out_tx.send(Outbound::Text(ack));
                                    }
                                    if handled.disconnect {
                                        // Server LB switch: ack then drop this
                                        // connection; run() reconnects immediately.
                                        break Err(Error::channel("server requested disconnect"));
                                    }
                                }
                                WsMessage::Pong(_) => {
                                    pong_at.store(
                                        chrono_now_millis(),
                                        std::sync::atomic::Ordering::SeqCst,
                                    );
                                }
                                WsMessage::Close(_) => break Err(Error::channel("connection closed by server")),
                                _ => {}
                            }
                        }
                    }
                }
            }
        };

        drop(out_tx);
        let _ = writer.await;
        result
    }

    /// Parse + handle one text frame. Returns the ack to send back (if any)
    /// and whether the server requested a disconnect.
    fn handle_frame(&self, raw: &str) -> FrameHandle {
        let f: Frame = match serde_json::from_str(raw) {
            Ok(f) => f,
            Err(e) => {
                self.cfg.debugf(format!("bad frame: {e}"));
                return FrameHandle::default();
            }
        };
        let topic = f.topic().to_string();

        // SYSTEM ping: echo data back in the ack.
        if f.r#type == SUB_SYSTEM && topic == "ping" {
            let mut ack = success_ack(f.message_id(), "");
            ack.data = f.data.clone();
            return FrameHandle {
                ack: Some(json!(ack).to_string()),
                disconnect: false,
            };
        }
        // SYSTEM disconnect: only closes the current connection (not stopped),
        // triggering Run's reconnect branch (E8).
        if f.r#type == SUB_SYSTEM && topic == "disconnect" {
            let ack = success_ack(f.message_id(), "");
            self.cfg.debugf("server requested disconnect; reconnecting");
            return FrameHandle {
                ack: Some(json!(ack).to_string()),
                disconnect: true,
            };
        }

        // ACK-first (official connector parity): confirm immediately, process
        // asynchronously so long agent tasks never trigger server redelivery.
        // Duplicate deliveries are absorbed by two-layer dedup (E6).
        let ack = success_ack(f.message_id(), "");
        let on_frame = self.on_frame.clone();
        tokio::spawn(async move {
            let _ = on_frame(f).await;
        });
        FrameHandle {
            ack: Some(json!(ack).to_string()),
            disconnect: false,
        }
    }
}

#[derive(Default)]
struct FrameHandle {
    ack: Option<String>,
    disconnect: bool,
}

fn chrono_now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Exponential backoff 1s→30s with jitter (Go parity).
pub(crate) fn backoff_delay(attempt: u32) -> Duration {
    let shift = attempt.min(16);
    let d = DEFAULT_RECONNECT_BASE.saturating_mul(1u32 << shift);
    let d = d.min(DEFAULT_RECONNECT_MAX);
    let jitter = rand::Rng::gen_range(&mut rand::thread_rng(), 0..1000) as u64;
    d + Duration::from_millis(jitter)
}

/// First non-loopback IPv4 address (best-effort; empty string on failure).
pub(crate) fn local_lan_ip() -> String {
    match local_ip_address::local_ip() {
        Ok(std::net::IpAddr::V4(v4)) if !v4.is_loopback() => v4.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_caps_at_30s_with_jitter_under_1s() {
        for attempt in 0..20u32 {
            let d = backoff_delay(attempt);
            assert!(d >= Duration::from_secs(1));
            assert!(d <= DEFAULT_RECONNECT_MAX + Duration::from_millis(1000));
        }
        // Monotonic growth until cap.
        let d1 = backoff_delay(0);
        let d5 = backoff_delay(5);
        assert!(d5 >= d1);
    }

    #[test]
    fn ack_shape_matches_spec() {
        let ack = success_ack("mid-1", "");
        assert_eq!(ack.code, 200);
        assert_eq!(ack.headers.get("messageId").unwrap(), "mid-1");
        assert_eq!(ack.data, r#"{"success":true}"#);
        let v: serde_json::Value = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["headers"]["contentType"], "application/json");

        let echo = success_ack("mid-2", "echo-data");
        assert_eq!(echo.data, "echo-data");
    }
}
