//! Minimal bisect test for the card-cycle hang.

use dingtalk_channel::{Channel, Config};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn bisect_card_hang() {
    let server = MockServer::start().await;
    let mut cfg = Config::new("cid", "secret");
    cfg.api_base = server.uri();
    cfg.oapi_base = server.uri();
    cfg.stream_throttle = std::time::Duration::from_millis(10);

    Mock::given(method("POST"))
        .and(path("/v1.0/oauth2/accessToken"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"accessToken":"t","expireIn":7200})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1.0/card/instances"))
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
    Mock::given(method("PUT"))
        .and(path("/v1.0/card/instances"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/v1.0/card/streaming"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&server)
        .await;

    let ch = Channel::new(cfg);
    ch.on_message(|_m, _r| Box::pin(async { Ok(()) }));

    let body = serde_json::json!({
        "conversationId": "c1", "conversationType": "1",
        "senderStaffId": "s1", "senderId": "s1",
        "sessionWebhook": format!("{}/wh", server.uri()),
        "createAt": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        "msgId": format!("m{}", rand_suffix()),
        "msgtype": "text", "text": {"content": "hi"},
    });
    let msg = dingtalk_channel::normalize::normalize_incoming(body.to_string().as_bytes()).unwrap();

    eprintln!("STEP 1: creating replier");
    let reply = ch.make_replier_for_test(std::sync::Arc::new(msg));
    eprintln!("STEP 2: calling stream()");
    let s = reply.stream().await.unwrap();
    eprintln!("STEP 3: append");
    s.append("hello ".to_string()).await.unwrap();
    eprintln!("STEP 4: append2");
    s.append("world".to_string()).await.unwrap();
    eprintln!("STEP 5: finish");
    s.finish(String::new()).await.unwrap();
    eprintln!("DONE");
}

fn rand_suffix() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}
