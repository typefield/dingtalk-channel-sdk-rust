//! Inbound message normalization (SPEC §3.1; Go `internal/normalize` port).

use crate::types::{
    AtUser, IncomingMessage, Mention, Resource, CONVERSATION_TYPE_DM, CONVERSATION_TYPE_GROUP,
};
use serde::Deserialize;

#[derive(Deserialize, Default)]
#[serde(default)]
struct BotCallbackData {
    #[serde(default, rename = "conversationId")]
    conversation_id: String,
    #[serde(default, rename = "atUsers")]
    at_users: Vec<AtUser>,
    #[serde(default, rename = "msgId")]
    msg_id: String,
    #[serde(default, rename = "senderNick")]
    sender_nick: String,
    #[serde(default, rename = "isAdmin")]
    is_admin: bool,
    #[serde(default, rename = "senderStaffId")]
    sender_staff_id: String,
    #[serde(default, rename = "sessionWebhookExpiredTime")]
    session_webhook_expired_time: i64,
    #[serde(default, rename = "createAt")]
    create_at: i64,
    #[serde(default, rename = "senderCorpId")]
    sender_corp_id: String,
    #[serde(default, rename = "conversationType")]
    conversation_type: String,
    #[serde(default, rename = "senderId")]
    sender_id: String,
    #[serde(default, rename = "conversationTitle")]
    conversation_title: String,
    #[serde(default, rename = "isInAtList")]
    is_in_at_list: bool,
    #[serde(default, rename = "sessionWebhook")]
    session_webhook: String,
    text: TextContent,
    #[serde(default, rename = "msgtype")]
    msg_type: String,
    #[serde(default)]
    content: serde_json::Value,
}

#[derive(Deserialize, Default)]
struct TextContent {
    #[serde(default)]
    content: String,
}

fn as_str(v: &serde_json::Value) -> &str {
    v.as_str().unwrap_or("")
}

fn normalize_conversation_type(t: &str) -> &'static str {
    if t == "1" {
        CONVERSATION_TYPE_DM
    } else {
        CONVERSATION_TYPE_GROUP
    }
}

/// Parse content by message type into (text, resources, mentions).
pub(crate) fn parse_content(
    msg_type: &str,
    raw_content: &serde_json::Value,
    at_users: &[AtUser],
) -> (String, Vec<Resource>, Vec<Mention>) {
    if raw_content.is_null() {
        return (String::new(), Vec::new(), Vec::new());
    }
    let m = match raw_content.as_object() {
        Some(m) => m,
        None => return (String::new(), Vec::new(), Vec::new()),
    };
    let empty: Vec<Resource> = Vec::new();
    let _ = at_users;
    match msg_type {
        "text" => (convert_text(m), empty.clone(), Vec::new()),
        "richText" => {
            let (t, mentions) = convert_rich_text(m);
            (t, empty, mentions)
        }
        "picture" => {
            let (t, r) = convert_picture(m);
            (t, r, Vec::new())
        }
        "file" => {
            let (t, r) = convert_file(m);
            (t, r, Vec::new())
        }
        "audio" => {
            let (t, r) = convert_audio(m);
            (t, r, Vec::new())
        }
        "video" => {
            let (t, r) = convert_video(m);
            (t, r, Vec::new())
        }
        "markdown" => (convert_markdown(m), empty, Vec::new()),
        "actionCard" => (convert_action_card(m), empty, Vec::new()),
        "interactiveCard" => (convert_interactive_card(m), empty, Vec::new()),
        "reply" => (convert_reply(m), empty, Vec::new()),
        other => {
            let _ = other;
            (
                m.get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                empty,
                Vec::new(),
            )
        }
    }
}

// ── converters_text ──

fn convert_text(m: &serde_json::Map<String, serde_json::Value>) -> String {
    m.get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn convert_markdown(m: &serde_json::Map<String, serde_json::Value>) -> String {
    m.get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

// ── converters_richtext ──

fn convert_rich_text(m: &serde_json::Map<String, serde_json::Value>) -> (String, Vec<Mention>) {
    let mut sb = String::new();
    let mut mentions = Vec::new();
    let arr = match m.get("richText").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return (String::new(), mentions),
    };
    for item in arr {
        let elem = match item.as_object() {
            Some(e) => e,
            None => continue,
        };
        let typ = elem.get("type").map(as_str).unwrap_or("");
        match typ {
            "text" => {
                sb.push_str(elem.get("text").map(as_str).unwrap_or(""));
            }
            "at" => {
                if let Some(uids) = elem.get("atUserIds").and_then(|v| v.as_array()) {
                    for uid in uids {
                        mentions.push(Mention {
                            user_id: uid.to_string().trim_matches('"').to_string(),
                            ..Default::default()
                        });
                    }
                }
                if let Some(mobiles) = elem.get("atMobiles").and_then(|v| v.as_array()) {
                    for mob in mobiles {
                        let s = mob.to_string().trim_matches('"').to_string();
                        mentions.push(Mention {
                            user_id: s.clone(),
                            name: s,
                            ..Default::default()
                        });
                    }
                }
            }
            _ => {}
        }
    }
    (sb, mentions)
}

// ── converters_media ──

fn convert_picture(m: &serde_json::Map<String, serde_json::Value>) -> (String, Vec<Resource>) {
    let dc = m.get("downloadCode").map(as_str).unwrap_or("");
    if !dc.is_empty() {
        return (
            "[图片]".into(),
            vec![Resource {
                r#type: "image".into(),
                download_code: dc.into(),
                ..Default::default()
            }],
        );
    }
    ("[图片]".into(), Vec::new())
}

fn convert_file(m: &serde_json::Map<String, serde_json::Value>) -> (String, Vec<Resource>) {
    let dc = m.get("downloadCode").map(as_str).unwrap_or("");
    let mut fn_ = m.get("fileName").map(as_str).unwrap_or("").to_string();
    let mut resources = Vec::new();
    if !dc.is_empty() {
        resources.push(Resource {
            r#type: "file".into(),
            download_code: dc.into(),
            file_name: fn_.clone(),
            ..Default::default()
        });
    }
    if fn_.is_empty() {
        fn_ = "文件".into();
    }
    (format!("[文件: {fn_}]"), resources)
}

fn convert_audio(m: &serde_json::Map<String, serde_json::Value>) -> (String, Vec<Resource>) {
    let dc = m.get("downloadCode").map(as_str).unwrap_or("");
    let fn_ = m.get("fileName").map(as_str).unwrap_or("").to_string();
    let rec = m.get("recognition").map(as_str).unwrap_or("").to_string();
    let mut resources = Vec::new();
    if !dc.is_empty() {
        resources.push(Resource {
            r#type: "audio".into(),
            download_code: dc.into(),
            file_name: fn_,
            recognition: rec.clone(),
        });
    }
    if !rec.is_empty() {
        return (rec, resources);
    }
    ("[语音消息]".into(), resources)
}

fn convert_video(m: &serde_json::Map<String, serde_json::Value>) -> (String, Vec<Resource>) {
    let dc = m.get("downloadCode").map(as_str).unwrap_or("");
    let fn_ = m.get("fileName").map(as_str).unwrap_or("").to_string();
    let mut resources = Vec::new();
    if !dc.is_empty() {
        resources.push(Resource {
            r#type: "video".into(),
            download_code: dc.into(),
            file_name: fn_,
            ..Default::default()
        });
    }
    ("[视频]".into(), resources)
}

// ── converters_card ──

fn convert_action_card(m: &serde_json::Map<String, serde_json::Value>) -> String {
    let title = m.get("title").map(as_str).unwrap_or("");
    let body = m.get("text").map(as_str).unwrap_or("");
    let mut parts: Vec<&str> = Vec::new();
    if !title.is_empty() {
        parts.push(title);
    }
    if !body.is_empty() {
        parts.push(body);
    }
    if let Some(urls) = m.get("actionUrlItemList").and_then(|v| v.as_array()) {
        let action_urls: Vec<&str> = urls
            .iter()
            .filter_map(|u| u.get("actionUrl").and_then(|v| v.as_str()))
            .filter(|s| !s.is_empty())
            .collect();
        if action_urls.len() == 1 {
            return format!("{}\n\n操作链接：{}", parts.join("\n\n"), action_urls[0]);
        } else if action_urls.len() > 1 {
            return format!(
                "{}\n\n操作链接：\n- {}",
                parts.join("\n\n"),
                action_urls.join("\n- ")
            );
        }
    }
    if !parts.is_empty() {
        return parts.join("\n\n");
    }
    "[actionCard消息]".into()
}

fn convert_interactive_card(m: &serde_json::Map<String, serde_json::Value>) -> String {
    let url = m.get("biz_custom_action_url").map(as_str).unwrap_or("");
    if !url.is_empty() {
        return format!("收到交互式卡片链接：{url}");
    }
    "[interactiveCard消息]".into()
}

// ── converters_reply ──

fn convert_reply(m: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut text = m.get("text").map(as_str).unwrap_or("").to_string();
    if let Some(rm) = m.get("repliedMsg").and_then(|v| v.as_object()) {
        let quoted = extract_quoted_text(rm);
        if !quoted.is_empty() {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&quoted);
        }
    }
    text
}

/// Build a summary of the quoted message; `content` may be a JSON string or object.
fn extract_quoted_text(replied_msg: &serde_json::Map<String, serde_json::Value>) -> String {
    let mt = replied_msg.get("msgType").map(as_str).unwrap_or("");
    let raw = match replied_msg.get("content") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) if !other.is_null() && !other.is_object() => other.to_string(),
        _ => String::new(),
    };
    let raw = raw.trim_matches('"').to_string();
    if raw.is_empty() {
        if let Some(c) = replied_msg.get("content").and_then(|v| v.as_object()) {
            return match mt {
                "text" => {
                    let t = c.get("text").map(as_str).unwrap_or("");
                    if !t.is_empty() {
                        format!("[引用] {t}")
                    } else {
                        quoted_default(mt)
                    }
                }
                _ => quoted_default(mt),
            };
        }
        return quoted_default(mt);
    }
    let c: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return format!("[引用] {raw}"),
    };
    match mt {
        "text" => {
            let t = c.get("text").and_then(|v| v.as_str()).unwrap_or("");
            format!("[引用] {t}")
        }
        "picture" => "[引用] [图片]".into(),
        "file" => {
            let f = c.get("fileName").and_then(|v| v.as_str()).unwrap_or("");
            format!("[引用] [文件: {f}]")
        }
        _ => quoted_default(mt),
    }
}

fn quoted_default(mt: &str) -> String {
    format!("[引用] {mt}消息")
}

// ── entry point ──

/// Normalize the raw bot-callback JSON payload into [`IncomingMessage`].
pub fn normalize_incoming(raw: &[u8]) -> crate::error::Result<IncomingMessage> {
    let d: BotCallbackData = serde_json::from_slice(raw)?;
    let ctype = normalize_conversation_type(&d.conversation_type);

    // Extract text/resources per message type.
    let (mut parsed_text, resources, mut mentions) =
        parse_content(&d.msg_type, &d.content, &d.at_users);

    // For text type keep reading from text.content for compatibility.
    if d.msg_type.is_empty() || d.msg_type == "text" {
        parsed_text = d.text.content.trim().to_string();
    }

    let mut text = parsed_text;
    if ctype == CONVERSATION_TYPE_GROUP {
        // In group chats @bot puts the prefix inside text.content — strip it (E5).
        if text.starts_with('@') {
            if let Some(i) = text.find(' ') {
                text = text[i + 1..].trim().to_string();
            }
        }
    }

    // Merge AtUsers into mentions.
    let mut mention_all = false;
    for au in &d.at_users {
        if au.staff_id == "all" || au.dingtalk_id == "all" {
            mention_all = true;
        }
        mentions.push(Mention {
            user_id: au.staff_id.clone(),
            name: au.staff_id.clone(),
            ..Default::default()
        });
    }

    let raw_value: serde_json::Value = serde_json::from_slice(raw)?;

    Ok(IncomingMessage {
        conversation_id: d.conversation_id,
        conversation_type: ctype.to_string(),
        conversation_title: d.conversation_title,
        sender_id: d.sender_id,
        sender_staff_id: d.sender_staff_id,
        sender_nick: d.sender_nick,
        sender_corp_id: d.sender_corp_id,
        text,
        msg_type: d.msg_type,
        content: d.content,
        resources,
        mentions,
        mention_all,
        at_users: d.at_users,
        session_webhook: d.session_webhook,
        webhook_expired_at: d.session_webhook_expired_time,
        msg_id: d.msg_id,
        create_at: d.create_at,
        is_admin: d.is_admin,
        is_in_at_list: d.is_in_at_list,
        batched_sources: Vec::new(),
        raw: raw_value,
    })
}
