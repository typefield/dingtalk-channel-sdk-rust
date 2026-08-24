//! Bot identity provider with cache (Go `bot_identity.go` port).

use crate::config::Config;
use crate::error::Result;
use crate::token::TokenProvider;
use crate::types::BotIdentity;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct RobotInfoResponse {
    #[serde(default)]
    robot_code: String,
    #[serde(default)]
    robot_name: String,
    #[serde(default)]
    avatar: String,
}

pub(crate) struct BotIdentityProvider {
    cfg: Arc<Config>,
    http: reqwest::Client,
    tokens: Arc<TokenProvider>,
    cache: tokio::sync::Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    identity: Option<Arc<BotIdentity>>,
    fetched_at: Option<Instant>,
    last_failure_at: Option<Instant>,
}

impl BotIdentityProvider {
    pub(crate) fn new(cfg: Arc<Config>, http: reqwest::Client, tokens: Arc<TokenProvider>) -> Self {
        Self {
            cfg,
            http,
            tokens,
            cache: tokio::sync::Mutex::new(CacheState::default()),
        }
    }

    /// Get bot identity with caching (30min TTL, 1min failure throttle).
    pub async fn get(&self) -> Option<Arc<BotIdentity>> {
        let mut st = self.cache.lock().await;
        let now = Instant::now();
        let fresh = match (st.identity.as_ref(), st.fetched_at) {
            (Some(_), Some(at)) => now.duration_since(at) < Duration::from_secs(30 * 60),
            _ => false,
        };
        if fresh {
            return st.identity.clone();
        }
        // Throttle refresh after a recent failure.
        if let Some(failed_at) = st.last_failure_at {
            if now.duration_since(failed_at) < Duration::from_secs(60) {
                return st.identity.clone();
            }
        }

        match self.fetch().await {
            Ok(identity) => {
                let identity = Arc::new(identity);
                st.identity = Some(identity.clone());
                st.fetched_at = Some(Instant::now());
                st.last_failure_at = None;
                Some(identity)
            }
            Err(e) => {
                st.last_failure_at = Some(now);
                if let Some(old) = &st.identity {
                    self.cfg.debugf(format!(
                        "failed to refresh bot identity, using stale cache: {e}"
                    ));
                    Some(old.clone())
                } else {
                    self.cfg
                        .debugf(format!("failed to fetch bot identity: {e}"));
                    None
                }
            }
        }
    }

    async fn fetch(&self) -> Result<BotIdentity> {
        let token = self.tokens.get().await?;
        let url = format!("{}/v1.0/robot/robotInfo", self.cfg.api_base);
        let resp = self
            .http
            .get(url)
            .header("x-acs-dingtalk-access-token", token)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let raw = resp.text().await?;
        if status != 200 {
            return Err(crate::error::Error::channel(format!(
                "robotInfo API failed: status={status} body={raw}"
            )));
        }
        let r: RobotInfoResponse = serde_json::from_str(&raw)?;
        Ok(BotIdentity {
            robot_code: if r.robot_code.is_empty() {
                self.cfg.client_id.clone()
            } else {
                r.robot_code
            },
            robot_name: if r.robot_name.is_empty() {
                "Bot".into()
            } else {
                r.robot_name
            },
            avatar: r.avatar,
        })
    }
}
