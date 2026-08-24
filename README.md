# DingTalk Channel SDK for Rust

A conversation access layer decoupled from any agent runtime. It provides the
DingTalk Stream connection, inbound message normalization, safety controls,
proactive sends, and streaming AI-card replies behind one high-level `Channel`.

Requires Rust 1.75 or newer.

## Example

```rust,no_run
use dingtalk_channel::{Channel, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::new(Config::new(
        std::env::var("DD_CLIENT_ID")?,
        std::env::var("DD_CLIENT_SECRET")?,
    ));

    channel.on_message(|message, reply| {
        Box::pin(async move { reply.text(format!("received: {}", message.text)).await })
    });

    channel.start().await?;
    Ok(())
}
```

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo +1.75.0 test --all-targets
```

The mocked integration suite covers HTTP callbacks, deduplication, the complete
AI-card streaming lifecycle and fallback, long-message chunking, proactive
sends, media upload, and policy rejection.

## License

MIT
