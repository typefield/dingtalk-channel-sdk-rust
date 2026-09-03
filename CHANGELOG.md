# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-09-04

### Added

- Stream long connection with auto-reconnect, exponential backoff and jitter.
- Inbound message normalization (group/DM, mentions, at-users, resources).
- Safety pipeline: per-chat serialization, dedup cache, policy gate, stale-lock
  recovery, SSRF guard with allowlist.
- Streaming AI-card replies with the five-step card protocol and webhook text
  fallback.
- Proactive sends, media upload, long-message chunking with fence-aware
  splitting, outbound retry and rate limiting.
- HTTP callback mode with HMAC-SHA256 signature verification.
- Mocked integration suite covering callbacks, dedup, card lifecycle, chunking,
  proactive sends, media upload and policy rejection.
