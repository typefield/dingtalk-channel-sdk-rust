# 发布 DingTalk Channel SDK for Rust

[English](RELEASING.md) | 简体中文

本 runbook 供维护者将 `dingtalk-channel-sdk` 发布到 crates.io 使用。
发布模型与 Lark channel SDK 家族一致:`main` 上一个通过评审的提交、一个
`v<version>` 注释 tag、由 tag 触发的发布工作流
(`.github/workflows/release.yml`)负责校验版本、重跑测试、发布到 crates.io,
并从 `CHANGELOG.md` 生成 GitHub Release。

crates.io 的版本不可变更。请把 tag 推送当作一次对外生产变更:推送前确认
版本号、提交内容、测试与元数据。

## 发布模型

- 唯一来源:`typefield/dingtalk-channel-sdk-rust` 的 `main` 上通过评审、
  CI 全绿(Test 1.75.0 / Test stable / Lint / Package)的提交。
- 发布标识:该提交上的注释 tag `v<version>`。tag 与 `Cargo.toml` 不一致时
  工作流会拒绝发布。
- 产物:crates.io 上的 `dingtalk-channel-sdk` crate,由 `Release` 工作流以
  `cargo publish --locked` 发布。
- 发布说明:`CHANGELOG.md` 的 `## [x.y.z]` 小节,由工作流自动抽取为
  GitHub Release 正文。
- MSRV:`Cargo.toml` 的 `rust-version = "1.75"` 是发布门禁(CI 含 1.75.0
  job)。

## 所需权限与本地前置条件

发布操作者需要具备以下全部条件:

1. GitHub 仓库 admin 权限:管理 `crates.io` environment 及其 secrets、
   推送 tag。
2. 一个有权限发布 `dingtalk-channel-sdk` 的 crates.io API token
   (crates.io → Account settings → API tokens)。token **只**保存在
   `crates.io` deployment environment 的 `CARGO_REGISTRY_TOKEN`
   environment secret 里;严禁提交入库、打印输出,或写在 `secrets.*`
   之外的 workflow `env:` 中。
3. Rust 工具链 1.75.0 与 stable,`cargo` 在 `PATH` 上。

一次性仓库配置(已完成的可跳过):

- 存在名为 `crates.io` 的 environment(`Settings → Environments`)。
- 其下已配置 `CARGO_REGISTRY_TOKEN` environment secret。

## 1. 准备发布提交

1. 从最新、干净的 `main` 开始;不要发布未经评审的本地改动。
2. 选定发布版本:不得与 crates.io 已有版本重复
   (`cargo search dingtalk-channel-sdk` 或官网查询)。
3. 更新 `Cargo.toml` 的 `version`,刷新 `Cargo.lock`(`cargo update -w` 或
   任意一次构建),并在 `CHANGELOG.md` 中新增 `## [x.y.z] - YYYY-MM-DD`
   小节(Keep a Changelog 格式;工作流按此小节原样生成发布说明)。
4. 有意的 API、行为、依赖、兼容性或安全变更必须记入 changelog。
5. 提交(如 `chore: prepare v0.1.0`),推送,等 `main` 上 CI 全绿。

## 2. 验证待发布的源码

在干净工作树中跑与 CI 相同的门禁:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --doc
cargo package --locked   # 发布演练;提前暴露打包问题
```

继续之前确认:

- 忽略构建产物后 `git status --short` 为空;
- `Cargo.toml` 版本与目标 tag 名(去掉 `v` 前缀)一致;
- `CHANGELOG.md` 中该版本小节存在且内容正确——它会被原样作为公开发布说明。

## 3. 给验证过的提交打 tag

使用注释 tag;替换为实际批准的版本号。不要移动或复用已发布的 tag。

```bash
git switch main
git pull --ff-only origin main
git tag -a v<version> -m "Release v<version>"
git push origin v<version>
```

tag 与 `Cargo.toml` 版本不一致时,工作流会在 "Verify tag matches Cargo.toml
version" 一步失败。仅当 tag 尚未用于发布时才允许删除重打;绝不要改动已被
消费方使用过的 tag。

## 4. 盯发布工作流

推送 tag 即触发 `Release` 工作流(Actions → Release → Publish to
crates.io),依次:

1. 校验 tag ↔ `Cargo.toml` 版本;
2. 执行 `cargo test --all-targets`;
3. 使用 `CARGO_REGISTRY_TOKEN` environment secret 执行
   `cargo publish --locked`;
4. 成功后执行 `GitHub Release` job(抽取 `CHANGELOG.md` 小节,基于 tag
   创建 Release)。

等待两个 job 全绿:

```bash
gh run watch "$(gh run list -R typefield/dingtalk-channel-sdk-rust \
  --workflow Release --limit 1 --json databaseId --jq '.[0].databaseId')"
```

crates.io 的搜索索引比 API 晚几分钟,不要因此重复打 tag 或重跑。

## 5. 验证公开发布结果

工作流成功后,在干净的消费方环境验证:

1. crates.io 展示了准确版本:
   `https://crates.io/crates/dingtalk-channel-sdk/<version>`;
2. 版本可被下游解析(在临时工程 `cargo add
   dingtalk-channel-sdk@<version>`,或
   `curl -s https://crates.io/api/v1/crates/dingtalk-channel-sdk/<version>`);
3. tag 上存在 GitHub Release,正文为预期的 changelog 内容;
4. `cargo install dingtalk-channel-sdk --version <version>`(或临时工程对其
   `cargo build`)成功——这验证的是打包产物而非本地检出。

## 失败处理与回滚

| 情况 | 处理 |
| --- | --- |
| 工作流在版本校验失败 | tag 与 `Cargo.toml` 不一致。仅删除未发布的 tag,修复后重打。 |
| `cargo publish` 失败(鉴权、限流、元数据缺失) | 修复原因后重推 tag(修复需在 tag 指向的提交上)。crates.io 不保留部分上传状态,同一版本重新发布会允许。 |
| 发布成功但发现缺陷 | crates.io 版本不可替换。发布修复后的更高版本;`cargo yank <坏版本>` 阻止新依赖选用,并在新版本发布说明中说明。 |
| 发布说明有误 | GitHub Release 正文可随时修改;crates.io 产物不可变。 |
| 发现安全问题 | 按 `SECURITY.md` 处理,公开披露前先协调发布修复版本。 |

绝不在消费方可能已使用后删除、改写或强推 tag。GitHub Release 文字可以改,
crates.io 产物不可变。

## 发布记录

每次发布保留一份仅维护者可见的记录,包含:

- 版本、tag、commit SHA 与时间戳;
- `Release` 工作流 run URL 及其测试/打包证据;
- crates.io 版本 URL 与消费方解析验证结果;
- GitHub Release URL;
- 已知限制、后续工作或 yank 建议。

记录中严禁出现 registry token 或任何其他凭证。
