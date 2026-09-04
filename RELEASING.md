# Releasing DingTalk Channel SDK for Rust

This runbook is for maintainers publishing `dingtalk-channel-sdk` to crates.io.
The release model follows the Lark channel SDK family: a reviewed commit on
`main`, an annotated `v<version>` tag, and a tag-triggered publishing workflow
(`.github/workflows/release.yml`) that verifies the version, reruns the tests,
publishes to crates.io, and cuts the GitHub Release from `CHANGELOG.md`.

Crates.io versions are immutable. Treat the tag push as an external production
change: review the version, commit, tests, and metadata before pushing it.

## Release model

- Source of truth: a reviewed commit on `main` in
  `typefield/dingtalk-channel-sdk-rust` with green CI (Test 1.75.0 / Test
  stable / Lint / Package).
- Release identity: an annotated Git tag named `v<version>` on that exact
  commit. The workflow refuses to publish when the tag does not match
  `Cargo.toml`.
- Artifact: the `dingtalk-channel-sdk` crate on crates.io, published with
  `cargo publish --locked` by the `Release` workflow.
- Release notes: the `## [x.y.z]` section of `CHANGELOG.md`, extracted
  automatically into the GitHub Release body.
- MSRV: `rust-version = "1.75"` in `Cargo.toml` is a release gate (CI runs a
  1.75.0 job).

## Required authority and local prerequisites

The release operator needs all of the following:

1. Admin access to the GitHub repository, to manage the `crates.io`
   environment and its secrets, and to push tags.
2. A crates.io API token authorized to publish `dingtalk-channel-sdk`
   (crates.io → Account settings → API tokens). The token is stored **only**
   as the `CARGO_REGISTRY_TOKEN` environment secret of the `crates.io`
   deployment environment. Never commit it, echo it, or place it in a workflow
   `env:` block outside `secrets.*`.
3. Rust toolchain 1.75.0 and stable, plus `cargo` on `PATH`.

One-time repository setup (already done unless the repo is recreated):

- The `crates.io` environment exists (`Settings → Environments`).
- `CARGO_REGISTRY_TOKEN` is set as an environment secret on it.

## 1. Prepare the release commit

1. Start from an up-to-date, clean `main` checkout. Do not release an
   unreviewed local change.
2. Select the release version. It must not already exist on crates.io
   (`cargo search dingtalk-channel-sdk` or the crates.io website).
3. Update `Cargo.toml` `version`, refresh `Cargo.lock` (`cargo update -w` or
   any build), and add the `## [x.y.z] - YYYY-MM-DD` section to
   `CHANGELOG.md` (Keep a Changelog format; the workflow extracts exactly this
   section as release notes).
4. Record intentional API, behavior, dependency, compatibility, or security
   changes in the changelog.
5. Commit (e.g. `chore: prepare v0.1.0`), push, and let CI go green on `main`.

## 2. Verify the exact release source

Run the same gates CI runs, from a clean tree:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --doc
cargo package --locked   # publish dry-run; catches packaging errors early
```

Confirm before continuing:

- `git status --short` is empty after ignoring generated build output;
- `Cargo.toml` version equals the intended tag name (minus the `v` prefix);
- the `CHANGELOG.md` section for this version exists and reads correctly —
  it becomes the public release notes verbatim.

## 3. Tag the verified commit

Use an annotated tag. Substitute the exact approved version; do not move or
reuse a published tag.

```bash
git switch main
git pull --ff-only origin main
git tag -a v<version> -m "Release v<version>"
git push origin v<version>
```

If the tag and `Cargo.toml` version differ, the workflow fails at the
"Verify tag matches Cargo.toml version" step. Delete only an unpublished,
just-created tag after confirming its exact target; never alter a tag that was
used for a public release.

## 4. Watch the publishing workflow

Pushing the tag starts the `Release` workflow
(`Actions → Release → Publish to crates.io`), which:

1. verifies tag ↔ `Cargo.toml` version;
2. runs `cargo test --all-targets`;
3. runs `cargo publish --locked` with the `CARGO_REGISTRY_TOKEN` environment
   secret;
4. on success, runs `GitHub Release` (extracts the `CHANGELOG.md` section and
   creates the release from the tag).

Watch the run until both jobs are green:

```bash
gh run watch "$(gh run list -R typefield/dingtalk-channel-sdk-rust \
  --workflow Release --limit 1 --json databaseId --jq '.[0].databaseId')"
```

Do not push a second tag or re-run merely because crates.io search has not
updated yet; the index lags behind the API by a few minutes.

## 5. Verify the public release

After the workflow succeeds, verify from a clean consumer context:

1. crates.io shows the exact version:
   `https://crates.io/crates/dingtalk-channel-sdk/<version>`;
2. the version resolves for a downstream crate
   (`cargo add dingtalk-channel-sdk@<version>` in a scratch project, or
   `curl -s https://crates.io/api/v1/crates/dingtalk-channel-sdk/<version>`);
3. the GitHub Release exists on the tag with the expected changelog body;
4. `cargo install dingtalk-channel-sdk --version <version>` (or a scratch
   `cargo build` against it) succeeds — this exercises the packaged artifact,
   not the local checkout.

## Failure handling and rollback

| Situation | Action |
| --- | --- |
| Workflow fails at version verification | Tag/`Cargo.toml` mismatch. Delete only the unpublished tag, fix, re-tag. |
| `cargo publish` fails (auth, rate limit, missing metadata) | Fix the cause; push a new tag only after the fix is on the tagged commit. crates.io keeps no partial state, so re-publishing the same version after a failed upload is allowed. |
| Publish succeeds but a defect is found | crates.io versions cannot be replaced. Publish a corrected, higher version; `cargo yank <bad-version>` to stop new dependents picking it up, and document the issue in the new release notes. |
| Release notes look wrong | Edit the GitHub Release text freely; the crates.io artifact cannot change. |
| A security issue is found | Follow `SECURITY.md` and coordinate a fixed release privately before public disclosure. |

Do not delete, rewrite, or force-push a tag after consumers may have used it.
GitHub Release text can be corrected, but the crates.io artifact is immutable.

## Release record

For each release, retain a maintainer-only record containing:

- release version, tag, commit SHA, and timestamp;
- the `Release` workflow run URL and its test/package evidence;
- crates.io version URL and the consumer resolution check;
- GitHub Release URL;
- known limitations, follow-up work, or yank guidance.

Never place the registry token, or any other credential, in the record.
