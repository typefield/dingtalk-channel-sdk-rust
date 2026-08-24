//! Access-token provider (SPEC §8; Go `token.go` port).

use crate::config::Config;
use crate::error::{Error, Result};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default, rename = "accessToken")]
    access_token: String,
    #[serde(default, rename = "expireIn")]
    expire_in: i64,
}

/// Fetch and cache the access token (refreshed 60s before expiry).
pub struct TokenProvider {
    cfg: Arc<Config>,
    http: reqwest::Client,
    state: tokio::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
    token: String,
    expires_at: Option<Instant>,
}

impl TokenProvider {
    pub(crate) fn new(cfg: Arc<Config>, http: reqwest::Client) -> Self {
        Self {
            cfg,
            http,
            state: tokio::sync::Mutex::new(State::default()),
        }
    }

    pub async fn get(&self) -> Result<String> {
        let mut st = self.state.lock().await;
        let fresh = st
            .expires_at
            .map(|e| Instant::now() + Duration::from_secs(60) < e)
            .unwrap_or(false);
        if !st.token.is_empty() && fresh {
            return Ok(st.token.clone());
        }

        let body = serde_json::json!({
            "appKey": self.cfg.client_id,
            "appSecret": self.cfg.client_secret,
        });
        let url = format!("{}/v1.0/oauth2/accessToken", self.cfg.api_base);
        let resp = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let raw = resp.text().await?;
        if status != 200 {
            return Err(Error::channel(format!("accessToken: http {status}: {raw}")));
        }
        let out: TokenResponse = serde_json::from_str(&raw)?;
        if out.access_token.is_empty() {
            return Err(Error::channel(format!(
                "accessToken: empty token in response: {raw}"
            )));
        }
        st.token = out.access_token;
        let expire_in = if out.expire_in <= 0 {
            7200
        } else {
            out.expire_in
        };
        st.expires_at = Some(Instant::now() + Duration::from_secs(expire_in as u64));
        Ok(st.token.clone())
    }
}
