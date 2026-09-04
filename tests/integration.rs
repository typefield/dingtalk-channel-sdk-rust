//! Integration tests against a mocked DingTalk API (wiremock).
//! Mirrors the Go SDK's channel_test / fallback_test / e7_media_test scenarios.

use dingtalk_channel::{Channel, Config, SendTarget};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use wiremock::matchers::{body_partial_json, body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_config(server: &MockServer) -> Config {
    let mut cfg = Config::new("test-client-id", "test-client-secret");
    cfg.api_base = server.uri();
    cfg.oapi_base = server.uri();
    // Speed up streaming for tests.
    cfg.stream_throttle = std::time::Duration::from_millis(10);
    cfg
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn callback_body(webhook: &str, text: &str, msg_id: &str) -> Value {
    json!({
        "conversationId": "cid-001",
        "conversationType": "1",
        "senderNick": "tester",
        "senderStaffId": "staff-1",
        "senderId": "sender-1",
        "sessionWebhook": webhook,
        "sessionWebhookExpiredTime": 0,
        "createAt": now_ms(),
        "msgId": msg_id,
        "msgtype": "text",
        "text": {"content": text},
        "isInAtList": false,
        "atUsers": [],
    })
}

async fn mount_token(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/v1.0/oauth2/accessToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "accessToken": "tok-123",
            "expireIn": 7200,
        })))
        .mount(server)
        .await;
}

/// Mount a counting endpoint: returns a handle to read hit counts.
async fn mount_counting(
    server: &MockServer,
    m: wiremock::http::Method,
    p: &str,
    body_contains: Option<&str>,
) -> Arc<AtomicUsize> {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter2 = counter.clone();
    let mut given = Mock::given(method(m.as_str())).and(path(p));
    if let Some(frag) = body_contains {
        given = given.and(body_string_contains(frag));
    }
    given
        .respond_with(move |_req: &wiremock::Request| {
            counter2.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_string("{}")
        })
        .mount(server)
        .await;
    counter
}

fn assert_count(counter: &Arc<AtomicUsize>, min: usize, what: &str) {
    let got = counter.load(Ordering::SeqCst);
    assert!(got >= min, "{what}: expected ≥{min}, got {got}");
}

// ── HTTP mode end-to-end: signature → handler → sessionWebhook reply ──

#[tokio::test]
async fn http_mode_echo_via_session_webhook() {
    let server = Arc::new(MockServer::start().await);
    mount_token(&server).await;
    let webhook_hits = mount_counting(
        &server,
        wiremock::http::Method::POST,
        "/webhook/reply",
        Some("sampleText"),
    )
    .await;

    let ch = Channel::new(test_config(&server));
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = seen.clone();
    ch.on_message(move |msg, reply| {
        let seen = seen2.clone();
        Box::pin(async move {
            seen.fetch_add(1, Ordering::SeqCst);
            assert_eq!(msg.text, "hello rust");
            reply.text(format!("received: {}", msg.text)).await
        })
    });

    let webhook_url = format!("{}/webhook/reply", server.uri());
    let body = callback_body(&webhook_url, "hello rust", "m-1").to_string();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".into());
    // sign = Base64(HmacSHA256(secret, ts + "\n" + secret))
    use base64::Engine;
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(b"test-client-secret").unwrap();
    mac.update(format!("{timestamp}\ntest-client-secret").as_bytes());
    let sign = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    ch.handle_http_callback(body.as_bytes(), &timestamp, &sign)
        .await
        .expect("callback should succeed");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_count(&webhook_hits, 1, "sessionWebhook reply");
}

#[tokio::test]
async fn http_mode_rejects_bad_signature() {
    let server = MockServer::start().await;
    let ch = Channel::new(test_config(&server));
    ch.on_message(|_msg, _reply| Box::pin(async { Ok(()) }));

    let body = callback_body("http://127.0.0.1:1/x", "hi", "m-1").to_string();
    let err = ch
        .handle_http_callback(body.as_bytes(), "1700000000", "bad-signature")
        .await;
    assert!(err.is_err(), "bad signature must be rejected");
}

// ── Dedup: same msgId delivered twice → handler once ──

#[tokio::test]
async fn dedup_drops_second_delivery() {
    let server = MockServer::start().await;
    let mut cfg = test_config(&server);
    cfg.safety.chat_queue.enabled = true;
    let ch = Channel::new(cfg);

    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = seen.clone();
    ch.on_message(move |_msg, _reply| {
        let seen = seen2.clone();
        Box::pin(async move {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    });

    // Same business msgId delivered under two different protocol IDs.
    let msg = dingtalk_channel::normalize::normalize_incoming(
        callback_body("http://127.0.0.1:1/x", "dup", "same-msg-id")
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    ch.process_incoming_for_test("proto-1", Arc::new(msg)).await;
    let msg2 = dingtalk_channel::normalize::normalize_incoming(
        callback_body("http://127.0.0.1:1/x", "dup", "same-msg-id")
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    ch.process_incoming_for_test("proto-2-different", Arc::new(msg2))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "second delivery must be deduped"
    );
}

// ── AI card full cycle (E1–E3): create → deliver → INPUTING → stream → FINISHED ──

#[tokio::test]
async fn card_streaming_full_cycle() {
    let server = Arc::new(MockServer::start().await);
    mount_token(&server).await;

    Mock::given(method("POST"))
        .and(path("/v1.0/card/instances"))
        .and(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1.0/card/instances/deliver"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"result":[{"success":true}]}"#),
        )
        .mount(&server)
        .await;

    let inputing_hits = Arc::new(AtomicUsize::new(0));
    let inputing2 = inputing_hits.clone();
    Mock::given(method("PUT"))
        .and(path("/v1.0/card/instances"))
        .respond_with(move |req: &wiremock::Request| {
            let body = std::str::from_utf8(&req.body).unwrap_or("");
            if body.contains("\"flowStatus\":\"2\"") {
                inputing2.fetch_add(1, Ordering::SeqCst);
            }
            ResponseTemplate::new(200).set_body_string("{}")
        })
        .mount(&server)
        .await;
    let streaming_hits = mount_counting(
        &server,
        wiremock::http::Method::PUT,
        "/v1.0/card/streaming",
        None,
    )
    .await;
    let webhook_hits = mount_counting(
        &server,
        wiremock::http::Method::POST,
        "/webhook/reply",
        None,
    )
    .await;

    let ch = Channel::new(test_config(&server));
    ch.on_message(|_msg, reply| {
        Box::pin(async move {
            let s = reply.stream().await?;
            assert!(
                s.card_delivered(),
                "card must be delivered in this scenario"
            );
            s.append("Hello ".to_string()).await?;
            s.append("world".to_string()).await?;
            s.finish(String::new()).await
        })
    });

    let msg = dingtalk_channel::normalize::normalize_incoming(
        callback_body(&format!("{}/webhook/reply", server.uri()), "go", "m-card")
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    ch.process_incoming_for_test("proto-card", Arc::new(msg))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert!(
        inputing_hits.load(Ordering::SeqCst) >= 1,
        "INPUTING status update expected"
    );
    assert!(
        streaming_hits.load(Ordering::SeqCst) >= 2,
        "content + final streaming frames expected"
    );
    assert_eq!(
        webhook_hits.load(Ordering::SeqCst),
        0,
        "card path must NOT fall back to webhook text"
    );
}

// ── Card create failure → silent degradation to webhook text (E4) ──

#[tokio::test]
async fn card_failure_falls_back_to_webhook_text() {
    let server = Arc::new(MockServer::start().await);
    mount_token(&server).await;
    // No /v1.0/card/instances mock → 404 → create fails.

    let webhook_hits = mount_counting(
        &server,
        wiremock::http::Method::POST,
        "/webhook/reply",
        Some("degraded but delivered"),
    )
    .await;

    let ch = Channel::new(test_config(&server));
    ch.on_message(|_msg, reply| {
        Box::pin(async move {
            let s = reply.stream().await?;
            assert!(!s.card_delivered(), "card create failed → degraded mode");
            s.append("degraded but delivered".to_string()).await?;
            s.finish(String::new()).await
        })
    });

    let msg = dingtalk_channel::normalize::normalize_incoming(
        callback_body(&format!("{}/webhook/reply", server.uri()), "go", "m-fb")
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    ch.process_incoming_for_test("proto-fb", Arc::new(msg))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_count(&webhook_hits, 1, "fallback webhook text");
}

// ── Oversized text is chunked at TextChunkLimit ──

#[tokio::test]
async fn oversized_text_is_chunked() {
    let server = Arc::new(MockServer::start().await);
    mount_token(&server).await;
    let webhook_hits = mount_counting(
        &server,
        wiremock::http::Method::POST,
        "/webhook/reply",
        None,
    )
    .await;

    let mut cfg = test_config(&server);
    cfg.text_chunk_limit = 10;
    let ch = Channel::new(cfg);
    ch.on_message(|_msg, reply| {
        Box::pin(async move {
            let long = "x".repeat(35);
            reply.text(long).await
        })
    });

    let msg = dingtalk_channel::normalize::normalize_incoming(
        callback_body(&format!("{}/webhook/reply", server.uri()), "go", "m-chunk")
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    ch.process_incoming_for_test("proto-chunk", Arc::new(msg))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(
        webhook_hits.load(Ordering::SeqCst) >= 4,
        "35 chars at limit 10 → ≥4 chunks"
    );
}

// ── Proactive send: DM and group endpoints ──

#[tokio::test]
async fn proactive_send_dm_and_group() {
    let server = Arc::new(MockServer::start().await);
    mount_token(&server).await;

    let dm_hits = Arc::new(AtomicUsize::new(0));
    let dm2 = dm_hits.clone();
    Mock::given(method("POST"))
        .and(path("/v1.0/robot/oToMessages/batchSend"))
        .and(body_partial_json(json!({"userIds": ["staff-9"]})))
        .respond_with(move |_r: &wiremock::Request| {
            dm2.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_string("{}")
        })
        .mount(&server)
        .await;
    let group_hits = Arc::new(AtomicUsize::new(0));
    let group2 = group_hits.clone();
    Mock::given(method("POST"))
        .and(path("/v1.0/robot/groupMessages/send"))
        .and(body_partial_json(
            json!({"openConversationId": "cid-group"}),
        ))
        .respond_with(move |_r: &wiremock::Request| {
            group2.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_string("{}")
        })
        .mount(&server)
        .await;

    let ch = Channel::new(test_config(&server));
    ch.on_message(|_m, _r| Box::pin(async { Ok(()) }));

    ch.send_text(&SendTarget::user("staff-9"), "dm hello")
        .await
        .unwrap();
    ch.send_markdown(&SendTarget::group("cid-group"), "title", "**md**")
        .await
        .unwrap();

    assert_eq!(dm_hits.load(Ordering::SeqCst), 1);
    assert_eq!(group_hits.load(Ordering::SeqCst), 1);
}

// ── Media upload strips leading '@' from media_id (E9) ──

#[tokio::test]
async fn media_upload_strips_at() {
    let server = Arc::new(MockServer::start().await);

    Mock::given(method("GET"))
        .and(path("/gettoken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errcode": 0,
            "access_token": "oapi-tok",
            "expires_in": 7200,
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/media/upload"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errcode": 0,
            "media_id": "@abc123",
            "type": "image",
            "created_at": 1700000000i64,
        })))
        .mount(&server)
        .await;

    let ch = Channel::new(test_config(&server));
    ch.on_message(|_m, _r| Box::pin(async { Ok(()) }));

    let msg = dingtalk_channel::normalize::normalize_incoming(
        callback_body(
            &format!("{}/webhook/reply", server.uri()),
            "upload",
            "m-media",
        )
        .to_string()
        .as_bytes(),
    )
    .unwrap();

    // Drive through the Reply API to exercise the full wiring.
    let reply = ch.make_replier_for_test(Arc::new(msg));
    let res = reply
        .upload_media(
            "image".into(),
            "pic.png".into(),
            "image/png".into(),
            vec![1, 2, 3],
        )
        .await
        .unwrap();
    assert_eq!(res.media_id, "abc123");
    assert_eq!(res.raw_media_id, "@abc123");
    assert_eq!(res.download_url, "https://down.dingtalk.com/media/abc123");
}

// ── Policy gate integration: blocklist rejects with event ──

#[tokio::test]
async fn policy_blocklist_emits_reject_event() {
    let server = MockServer::start().await;
    let mut cfg = test_config(&server);
    cfg.safety.policy.dm_mode = "blocklist".into();
    cfg.safety.policy.dm_blocklist = vec!["sender-1".into()];
    let ch = Channel::new(cfg);

    let rejected = Arc::new(AtomicUsize::new(0));
    let rejected2 = rejected.clone();
    ch.on_reject(move |e| {
        assert_eq!(e.reason.as_str(), "dm_blocked");
        rejected2.fetch_add(1, Ordering::SeqCst);
    });
    let handled = Arc::new(AtomicUsize::new(0));
    let handled2 = handled.clone();
    ch.on_message(move |_m, _r| {
        let h = handled2.clone();
        Box::pin(async move {
            h.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    });

    let msg = dingtalk_channel::normalize::normalize_incoming(
        callback_body("http://127.0.0.1:1/x", "blocked?", "m-blk")
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    ch.process_incoming_for_test("proto-blk", Arc::new(msg))
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
    assert_eq!(handled.load(Ordering::SeqCst), 0);
}

// ── richText resource extraction (lark attachment-zone port) ──

fn rich_text_body(msg_id: &str, segments: Value) -> Value {
    let mut body = callback_body("", "", msg_id);
    body["msgtype"] = json!("richText");
    body["content"] = json!({ "richText": segments });
    body
}

#[test]
fn rich_text_extracts_picture_and_file_resources() {
    let data = rich_text_body(
        "rt-1",
        json!([
            { "type": "text", "text": "图1 " },
            { "type": "picture", "picture": "dc-1" },
            { "type": "picture", "picture": "dc-1" },
            { "type": "picture", "picture": "dc-2" },
            { "type": "file", "downloadCode": "dc-3", "fileName": "report.pdf" },
            { "type": "text", "text": " 图2" }
        ]),
    )
    .to_string();
    let msg = dingtalk_channel::normalize::normalize_incoming(data.as_bytes()).unwrap();
    assert_eq!(msg.text, "图1  图2");
    assert_eq!(msg.resources.len(), 3, "duplicate dc-1 must be deduped");
    assert_eq!(msg.resources[0].r#type, "image");
    assert_eq!(msg.resources[0].download_code, "dc-1");
    assert_eq!(msg.resources[2].r#type, "file");
    assert_eq!(msg.resources[2].download_code, "dc-3");
    assert_eq!(msg.resources[2].file_name, "report.pdf");
}

#[test]
fn rich_text_dirty_segments_yield_no_resources() {
    let data = rich_text_body(
        "rt-2",
        json!([
            { "type": "picture", "picture": 123 },
            { "type": "picture", "picture": "" },
            { "type": "picture" },
            { "type": "file", "downloadCode": 42 },
            { "type": "text", "text": "ok" },
            "junk-segment"
        ]),
    )
    .to_string();
    let msg = dingtalk_channel::normalize::normalize_incoming(data.as_bytes()).unwrap();
    assert_eq!(msg.text, "ok");
    assert!(
        msg.resources.is_empty(),
        "dirty segments must not produce resources"
    );
}

// ── download_file_to_file (lark downloadResourceToFile port) ──

const MEDIA_BYTES: &[u8] = b"dingtalk-media-bytes-rust-port";

async fn mount_media(server: &MockServer, media_status: u16) {
    mount_token(server).await;
    let url = json!({ "downloadUrl": format!("{}/media.bin", server.uri()) }).to_string();
    Mock::given(method("GET"))
        .and(path("/v1.0/robot/messageFiles/download"))
        .respond_with(ResponseTemplate::new(200).set_body_string(url))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/media.bin"))
        .respond_with(ResponseTemplate::new(media_status).set_body_bytes(MEDIA_BYTES))
        .mount(server)
        .await;
}

fn media_channel_config(server: &MockServer) -> Config {
    let mut cfg = test_config(server);
    cfg.ssrf_allowlist = vec!["127.0.0.1".to_string()];
    cfg
}

#[tokio::test]
async fn download_file_to_file_streams_to_disk() {
    let server = MockServer::start().await;
    mount_media(&server, 200).await;
    let ch = Channel::new(media_channel_config(&server));

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("media.bin");
    let n = ch
        .download_file_to_file("dc-1", "m-1", &dest)
        .await
        .expect("download_file_to_file");
    assert_eq!(n, MEDIA_BYTES.len() as u64);
    assert_eq!(tokio::fs::read(&dest).await.unwrap(), MEDIA_BYTES);

    // The in-memory variant keeps working after the refactor.
    let bytes = ch
        .download_file("dc-1", "m-1")
        .await
        .expect("download_file");
    assert_eq!(bytes, MEDIA_BYTES);

    // No temp files left behind: only the downloaded file itself.
    let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
    let mut count = 0;
    while let Some(e) = entries.next_entry().await.unwrap() {
        assert_eq!(e.file_name(), std::ffi::OsString::from("media.bin"));
        count += 1;
    }
    assert_eq!(count, 1);
}

#[tokio::test]
async fn download_file_to_file_missing_parent_dir_fails_cleanly() {
    let server = MockServer::start().await;
    mount_media(&server, 200).await;
    let ch = Channel::new(media_channel_config(&server));

    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("no-such-dir").join("media.bin");
    let err = ch
        .download_file_to_file("dc-1", "m-1", &missing)
        .await
        .expect_err("missing parent dir must fail");
    assert!(format!("{err}").contains("cannot create temp file"));
    assert!(!dir.path().join("no-such-dir").exists());
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(entries.len(), 0, "no temp files may be left behind");
}

#[tokio::test]
async fn download_file_to_file_surfaces_non_200() {
    let server = MockServer::start().await;
    mount_media(&server, 404).await;
    let ch = Channel::new(media_channel_config(&server));

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("x.bin");
    let err = ch
        .download_file_to_file("dc-1", "m-1", &dest)
        .await
        .expect_err("non-200 must fail");
    assert!(format!("{err}").contains("http 404"));
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(entries.len(), 0, "no temp files may be left behind");
}
