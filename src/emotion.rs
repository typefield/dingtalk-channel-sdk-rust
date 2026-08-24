//! Message emotion stamps ("🤔思考中" status stickers) — openclaw connector
//! addEmotionReply/recallEmotionReply parity (Go `emotion.go` port).
//!
//! Stamps are not cards: they are text emotions pinned onto the user's message.
//! Lifecycle: MarkThinking on receipt → RecallThinking / MarkDone after reply.
//! Best-effort only — never fail a reply because of a stamp.

use crate::error::{Error, Result};
use serde_json::json;

pub const EMOTION_THINKING: &str = "🤔思考中";
pub const EMOTION_DONE: &str = "🥳完成";

impl crate::channel::Channel {
    async fn send_emotion(
        &self,
        conversation_id: &str,
        msg_id: &str,
        name: &str,
        recall: bool,
    ) -> Result<()> {
        if conversation_id.is_empty() || msg_id.is_empty() {
            return Err(Error::channel("emotion needs openConversationId and openMsgId"));
        }
        let path = if recall { "/v1.0/robot/emotion/recall" } else { "/v1.0/robot/emotion/reply" };
        let body = json!({
            "robotCode": self.cfg.client_id,
            "openConversationId": conversation_id,
            "openMsgId": msg_id,
            "emotionType": 2,
            "emotionName": name,
            "textEmotion": {
                "emotionId": "2659900",
                "emotionName": name,
                "text": name,
                "backgroundId": "im_bg_1",
            },
        });
        let cancel = tokio_util::sync::CancellationToken::new();
        self.cards.call(&cancel, reqwest::Method::POST, path, Some(&body)).await
    }

    /// Pin the "🤔思考中" stamp on the user's message when processing starts.
    /// Only works on human-sent messages (bot messages 500).
    pub async fn mark_thinking(&self, conversation_id: &str, msg_id: &str) -> Result<()> {
        self.send_emotion(conversation_id, msg_id, EMOTION_THINKING, false).await
    }

    /// Recall the "🤔思考中" stamp after the reply lands (best-effort).
    pub async fn recall_thinking(&self, conversation_id: &str, msg_id: &str) -> Result<()> {
        self.send_emotion(conversation_id, msg_id, EMOTION_THINKING, true).await
    }

    /// Swap "🤔思考中" for "🥳完成" after the reply lands (best-effort).
    pub async fn mark_done(&self, conversation_id: &str, msg_id: &str) -> Result<()> {
        let _ = self.send_emotion(conversation_id, msg_id, EMOTION_THINKING, true).await;
        self.send_emotion(conversation_id, msg_id, EMOTION_DONE, false).await
    }
}
