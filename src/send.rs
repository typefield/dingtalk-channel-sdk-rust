//! Proactive send: text/markdown/file/video/audio/image without an inbound
//! message (SPEC §4a; Go `send.go` port).

use crate::card::CardClient;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::reply::first_line_title;
use crate::types::{IncomingMessage, OutboundConfig};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;

/// Proactive send target. Exactly one of `user_id` (DM) or
/// `conversation_id` (group openConversationId) must be set.
#[derive(Debug, Clone, Default)]
pub struct SendTarget {
    pub user_id: String,
    pub conversation_id: String,

    /// Group @ capabilities (official params verified by dws).
    pub at_user_ids: Vec<String>,
    pub at_dingtalk_ids: Vec<String>,
    pub at_all: bool,
}

impl SendTarget {
    pub fn user(id: impl Into<String>) -> Self {
        Self {
            user_id: id.into(),
            ..Default::default()
        }
    }
    pub fn group(conversation_id: impl Into<String>) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            ..Default::default()
        }
    }

    pub(crate) fn valid(&self) -> bool {
        self.user_id.is_empty() != self.conversation_id.is_empty()
    }
}

/// Standalone proactive sender shared by the public Channel API and the
/// webhook-failure fallback path.
#[derive(Clone)]
pub(crate) struct ProactiveSender {
    pub cfg: Arc<Config>,
    pub cards: Arc<CardClient>,
}

impl ProactiveSender {
    pub(crate) fn new(cfg: Arc<Config>, cards: Arc<CardClient>) -> Self {
        Self { cfg, cards }
    }

    /// Webhook-failure fallback entry: derive target from the inbound message
    /// (group → conversationId; DM → sender).
    pub(crate) async fn reply_fallback(
        &self,
        msg: &Arc<IncomingMessage>,
        msg_key: &str,
        msg_param: Value,
    ) -> Result<()> {
        let target = if msg.conversation_type == crate::types::CONVERSATION_TYPE_GROUP {
            SendTarget::group(msg.conversation_id.clone())
        } else {
            let uid = if msg.sender_id.is_empty() {
                msg.sender_staff_id.clone()
            } else {
                msg.sender_id.clone()
            };
            SendTarget::user(uid)
        };
        self.send(&target, msg_key, msg_param).await
    }

    pub(crate) async fn send(
        &self,
        target: &SendTarget,
        msg_key: &str,
        msg_param: Value,
    ) -> Result<()> {
        if !target.valid() {
            return Err(Error::channel(
                "SendTarget: 恰好设置 UserID（单聊）或 ConversationID（群聊）之一",
            ));
        }

        // Outbound hooks + unified footer.
        let target_id = if target.user_id.is_empty() {
            &target.conversation_id
        } else {
            &target.user_id
        };
        let msg_param = apply_outbound(self.cfg.outbound.as_ref(), msg_key, msg_param, target_id);

        let res = self.send_inner(target, msg_key, msg_param).await;
        if let Some(hook) = self
            .cfg
            .outbound
            .as_ref()
            .and_then(|o| o.after_send.clone())
        {
            match &res {
                Ok(()) => hook("send", target_id, true, ""),
                Err(e) => hook("send", target_id, false, &e.to_string()),
            }
        }
        res
    }

    async fn send_inner(&self, target: &SendTarget, msg_key: &str, msg_param: Value) -> Result<()> {
        // msgParam must be stringified JSON (official docs).
        let param_str = msg_param.to_string();

        let opts = crate::outbound::RetryOptions {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
        };
        let cancel = tokio_util::sync::CancellationToken::new();

        if !target.user_id.is_empty() {
            let body = json!({
                "robotCode": self.cfg.client_id,
                "userIds": [target.user_id],
                "msgKey": msg_key,
                "msgParam": param_str,
            });
            let this = self.clone();
            let retry_cancel = cancel.clone();
            return crate::outbound::retry(
                &retry_cancel,
                move |_| {
                    let body = body.clone();
                    let this = this.clone();
                    let cancel = cancel.clone();
                    async move {
                        this.cards
                            .call(
                                &cancel,
                                reqwest::Method::POST,
                                "/v1.0/robot/oToMessages/batchSend",
                                Some(&body),
                            )
                            .await
                    }
                },
                &opts,
            )
            .await;
        }

        let mut body = json!({
            "robotCode": self.cfg.client_id,
            "openConversationId": target.conversation_id,
            "msgKey": msg_key,
            "msgParam": param_str,
        });
        if !target.at_user_ids.is_empty() {
            body["atUserIds"] = json!(target.at_user_ids);
        }
        if !target.at_dingtalk_ids.is_empty() {
            body["atOpendingtalkIds"] = json!(target.at_dingtalk_ids);
        }
        if target.at_all {
            body["isAtAll"] = json!(true);
        }
        let this = self.clone();
        let retry_cancel = cancel.clone();
        crate::outbound::retry(
            &retry_cancel,
            move |_| {
                let body = body.clone();
                let this = this.clone();
                let cancel = cancel.clone();
                async move {
                    this.cards
                        .call(
                            &cancel,
                            reqwest::Method::POST,
                            "/v1.0/robot/groupMessages/send",
                            Some(&body),
                        )
                        .await
                }
            },
            &opts,
        )
        .await
    }
}

fn apply_outbound(
    outbound: Option<&Arc<OutboundConfig>>,
    msg_key: &str,
    mut param: Value,
    target_id: &str,
) -> Value {
    let Some(outbound) = outbound else {
        return param;
    };
    if !outbound.footer.is_empty() {
        if let Value::Object(m) = &mut param {
            if msg_key == "sampleText" {
                if let Some(c) = m
                    .get_mut("content")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                {
                    m.insert(
                        "content".into(),
                        Value::String(format!("{c}\n\n{}", outbound.footer)),
                    );
                }
            } else if msg_key == "sampleMarkdown" {
                if let Some(t) = m
                    .get_mut("text")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                {
                    m.insert(
                        "text".into(),
                        Value::String(format!("{t}\n\n---\n{}", outbound.footer)),
                    );
                }
            }
        }
    }
    if let Some(before) = &outbound.before_send {
        let payload = param.to_string();
        if let Some(replaced) = before("send", target_id, &payload) {
            if let Ok(v) = serde_json::from_str::<Value>(&replaced) {
                param = v;
            }
        }
    }
    param
}

impl crate::channel::Channel {
    /// Proactively send text.
    pub async fn send_text(&self, target: &SendTarget, content: impl Into<String>) -> Result<()> {
        let mut p = Map::new();
        p.insert("content".into(), Value::String(content.into()));
        self.core
            .sender
            .send(target, "sampleText", Value::Object(p))
            .await
    }

    /// Proactively send markdown.
    pub async fn send_markdown(
        &self,
        target: &SendTarget,
        title: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<()> {
        let mut title: String = title.into();
        let text: String = text.into();
        if title.is_empty() {
            title = first_line_title(&text);
        }
        let mut p = Map::new();
        p.insert("title".into(), Value::String(title));
        p.insert("text".into(), Value::String(text));
        self.core
            .sender
            .send(target, "sampleMarkdown", Value::Object(p))
            .await
    }

    /// Send a file message (sampleFile): deliver uploaded media (RawMediaID with `@`)
    /// as a downloadable file — the reliable way to land images in chats
    /// (streaming card content cannot embed images; protocol limitation).
    pub async fn send_file(
        &self,
        target: &SendTarget,
        file_name: impl Into<String>,
        raw_media_id: impl Into<String>,
    ) -> Result<()> {
        let mut p = Map::new();
        p.insert("mediaId".into(), Value::String(raw_media_id.into()));
        p.insert("fileName".into(), Value::String(file_name.into()));
        p.insert("fileType".into(), Value::String("png".into()));
        self.core
            .sender
            .send(target, "sampleFile", Value::Object(p))
            .await
    }

    /// Send a video message (sampleVideo). Media IDs are RawMediaIDs (with `@`).
    pub async fn send_video(
        &self,
        target: &SendTarget,
        raw_video_media_id: impl Into<String>,
        raw_pic_media_id: impl Into<String>,
        duration_ms: i64,
    ) -> Result<()> {
        let duration_ms = if duration_ms <= 0 { 60000 } else { duration_ms };
        let mut p = Map::new();
        p.insert("duration".into(), Value::String(duration_ms.to_string()));
        p.insert(
            "videoMediaId".into(),
            Value::String(raw_video_media_id.into()),
        );
        p.insert("videoType".into(), Value::String("mp4".into()));
        p.insert("picMediaId".into(), Value::String(raw_pic_media_id.into()));
        self.core
            .sender
            .send(target, "sampleVideo", Value::Object(p))
            .await
    }

    /// Send an audio message (sampleAudio).
    pub async fn send_audio(
        &self,
        target: &SendTarget,
        raw_media_id: impl Into<String>,
        duration_ms: i64,
    ) -> Result<()> {
        let duration_ms = if duration_ms <= 0 { 60000 } else { duration_ms };
        let mut p = Map::new();
        p.insert("mediaId".into(), Value::String(raw_media_id.into()));
        p.insert("duration".into(), Value::String(duration_ms.to_string()));
        self.core
            .sender
            .send(target, "sampleAudio", Value::Object(p))
            .await
    }

    /// Send an image by public URL (OAPI-uploaded mediaIds have no public URL;
    /// use [`Self::send_file`] for local images).
    pub async fn send_image(
        &self,
        target: &SendTarget,
        image_url: impl Into<String>,
    ) -> Result<()> {
        let mut p = Map::new();
        p.insert("photoURL".into(), Value::String(image_url.into()));
        self.core
            .sender
            .send(target, "sampleImageMsg", Value::Object(p))
            .await
    }
}
