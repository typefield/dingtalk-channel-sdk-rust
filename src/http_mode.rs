//! HTTP callback mode (dispatcher shape): the SDK verifies signatures and
//! dispatches messages; the HTTP service is provided externally (serverless
//! entry, enterprise gateway). Replies still go through the sessionWebhook in
//! the payload; the processing pipeline is fully shared with Stream mode
//! (Go `http_mode.go` port).

use crate::channel::Channel;
use crate::error::{Error, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;

/// Verify the DingTalk HTTP-mode callback signature:
/// sign = Base64(HmacSHA256(key=appSecret, msg=timestamp+"\n"+appSecret)),
/// plus timestamp tolerance window (<=0 disables the window check).
/// Timestamps in seconds or milliseconds are both accepted.
pub fn verify_http_sign(
    secret: &str,
    timestamp: &str,
    sign: &str,
    tolerance: std::time::Duration,
) -> Result<()> {
    if sign.is_empty() || timestamp.is_empty() {
        return Err(Error::channel("http mode: missing timestamp/sign headers"));
    }
    let mut ts: i64 = timestamp
        .parse()
        .map_err(|_| Error::channel("http mode: invalid timestamp header"))?;
    if ts < 100_000_000_000 {
        ts *= 1000; // seconds → millis
    }
    if !tolerance.is_zero() {
        let now_ms = now_millis();
        let age = (now_ms - ts).abs();
        if age > tolerance.as_millis() as i64 {
            return Err(Error::channel(
                "http mode: timestamp outside tolerance window",
            ));
        }
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|e| Error::channel(format!("hmac init: {e}")))?;
    mac.update(format!("{timestamp}\n{secret}").as_bytes());
    let want = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    // Constant-time-ish comparison via byte-wise fold.
    if want.len() != sign.len()
        || want
            .bytes()
            .zip(sign.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            != 0
    {
        return Err(Error::channel("http mode: signature mismatch"));
    }
    Ok(())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Channel {
    /// Handle one HTTP-mode callback body (enterprise-internal robot; body is
    /// isomorphic with Stream's data payload). Signature failure or bad payload
    /// returns Err (caller should respond 401/400); business handling matches
    /// Stream mode — errors are swallowed for fast return.
    pub async fn handle_http_callback(
        &self,
        body: &[u8],
        timestamp: &str,
        sign: &str,
    ) -> Result<()> {
        if self.core.on_message.read().unwrap().is_none()
            && self.core.on_batch.read().unwrap().is_none()
        {
            return Err(Error::channel("OnMessage handler not registered"));
        }
        verify_http_sign(
            &self.core.cfg.client_secret,
            timestamp,
            sign,
            self.core.cfg.http_timestamp_tolerance,
        )?;
        let msg = crate::normalize::normalize_incoming(body)
            .map_err(|e| Error::channel(format!("http mode: bad bot message payload: {e}")))?;
        // HTTP callbacks carry no protocol-level delivery ID; both dedup layers
        // fall back to msgId. Retry-induced duplicates are absorbed idempotently.
        let proto_id = msg.msg_id.clone();
        self.process_incoming(&proto_id, Arc::new(msg)).await;
        Ok(())
    }
}
