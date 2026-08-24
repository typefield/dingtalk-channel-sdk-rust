//! Stream wire protocol structures (SPEC §2; Go `frame.go` port).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const SUB_CALLBACK: &str = "CALLBACK";
pub const SUB_SYSTEM: &str = "SYSTEM";

#[derive(Debug, Clone, Serialize)]
pub struct Subscription {
    #[serde(rename = "type")]
    pub r#type: String,
    pub topic: String,
}

#[derive(Debug, Serialize)]
pub struct OpenConnectionRequest {
    #[serde(rename = "clientId")]
    pub client_id: String,
    #[serde(rename = "clientSecret")]
    pub client_secret: String,
    pub subscriptions: Vec<Subscription>,
    pub ua: String,
    #[serde(skip_serializing_if = "String::is_empty", rename = "localIp")]
    pub local_ip: String,
}

#[derive(Debug, Deserialize)]
pub struct OpenConnectionResponse {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub ticket: String,
}

/// A server-pushed frame.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Frame {
    #[serde(rename = "specVersion", default)]
    pub spec_version: String,
    #[serde(rename = "type", default)]
    pub r#type: String,
    #[serde(default)]
    pub time: i64,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub data: String,
}

impl Frame {
    pub fn topic(&self) -> &str {
        self.headers.get("topic").map(|s| s.as_str()).unwrap_or("")
    }
    pub fn message_id(&self) -> &str {
        self.headers
            .get("messageId")
            .map(|s| s.as_str())
            .unwrap_or("")
    }
}

/// ACK sent back to the server (must reply or messages are redelivered).
#[derive(Debug, Serialize)]
pub struct FrameAck {
    pub code: i32,
    pub headers: HashMap<String, String>,
    pub message: String,
    pub data: String,
}

pub fn success_ack(message_id: &str, data: &str) -> FrameAck {
    let data = if data.is_empty() {
        r#"{"success":true}"#.to_string()
    } else {
        data.to_string()
    };
    FrameAck {
        code: 200,
        headers: HashMap::from([
            ("contentType".to_string(), "application/json".to_string()),
            ("messageId".to_string(), message_id.to_string()),
        ]),
        message: "ok".into(),
        data,
    }
}
