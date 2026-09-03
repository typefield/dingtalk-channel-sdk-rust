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

## Release

CI runs the checks above on every push and pull request (Rust 1.75 + stable),
plus a `cargo package --locked` publish dry-run. To release:

1. Update the version in `Cargo.toml` and add a `CHANGELOG.md` entry.
2. Push a tag `vX.Y.Z` — the release workflow verifies the tag matches
   `Cargo.toml`, runs the tests, publishes to crates.io (requires the
   `CARGO_REGISTRY_TOKEN` secret in the `crates.io` environment), and creates
   a GitHub release from the changelog.

## License

MIT
