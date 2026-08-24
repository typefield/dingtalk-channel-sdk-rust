//! Message admission policy gate (SPEC §4b; Go `safety/policy_gate.go` port).

use crate::types::{
    BotIdentity, GroupOverride, IncomingMessage, PolicyConfig, PolicyDecision, RejectReason,
    CONVERSATION_TYPE_GROUP,
};
use std::sync::RwLock;

pub struct PolicyGate {
    inner: RwLock<Inner>,
}

struct Inner {
    cfg: PolicyConfig,
    bot: Option<BotIdentity>,
}

impl PolicyGate {
    pub fn new(cfg: PolicyConfig) -> Self {
        Self {
            inner: RwLock::new(Inner { cfg, bot: None }),
        }
    }

    /// Evaluate whether a message passes admission policy.
    ///
    /// Order (four-language parity):
    /// 1. Admin bypass (highest priority)
    /// 2. Global deny list
    /// 3. Global allow list (when set, must be member)
    /// 4. Per conversation type: group / DM evaluation
    pub fn evaluate(&self, msg: &IncomingMessage) -> PolicyDecision {
        let inner = self.inner.read().unwrap();
        let cfg = &inner.cfg;

        // 1. Admin bypass.
        if is_member(&cfg.admins, &msg.sender_staff_id) {
            return PolicyDecision::allow();
        }
        // 2. Global deny list.
        if !msg.sender_staff_id.is_empty() && is_member(&cfg.deny_from, &msg.sender_staff_id) {
            return PolicyDecision::reject(RejectReason::SenderDenied);
        }
        // 3. Global allow list.
        if !(cfg.allow_from.is_empty()
            || msg.sender_staff_id.is_empty()
            || is_member(&cfg.allow_from, &msg.sender_staff_id))
        {
            return PolicyDecision::reject(RejectReason::SenderNotAllowed);
        }

        if msg.conversation_type == CONVERSATION_TYPE_GROUP {
            evaluate_group(cfg, msg)
        } else {
            evaluate_dm(cfg, msg)
        }
    }

    pub fn update_config(&self, cfg: PolicyConfig) {
        self.inner.write().unwrap().cfg = cfg;
    }

    pub fn get_config(&self) -> PolicyConfig {
        self.inner.read().unwrap().cfg.clone()
    }

    pub fn set_bot_identity(&self, bot: BotIdentity) {
        self.inner.write().unwrap().bot = Some(bot);
    }
}

fn is_member(list: &[String], id: &str) -> bool {
    !id.is_empty() && list.iter().any(|m| m == id)
}

fn evaluate_group(cfg: &PolicyConfig, msg: &IncomingMessage) -> PolicyDecision {
    // Blocklist first — group overrides can never exempt it.
    if is_member(&cfg.group_blocklist, &msg.conversation_id) {
        return PolicyDecision::reject(RejectReason::GroupBlocked);
    }

    let ov: Option<&GroupOverride> = cfg.group_overrides.get(&msg.conversation_id);

    // Allowlist: global hit OR explicit per-group entry admits the chat.
    if !cfg.group_allowlist.is_empty()
        && ov.is_none()
        && !is_member(&cfg.group_allowlist, &msg.conversation_id)
    {
        return PolicyDecision::reject(RejectReason::GroupNotAllowed);
    }

    if let Some(ov) = ov {
        if ov.enabled == Some(false) {
            return PolicyDecision::reject(RejectReason::GroupDisabled);
        }
    }

    // @-mention requirement (group override wins over global).
    let require_mention = ov
        .and_then(|o| o.require_mention)
        .or(cfg.require_mention)
        .unwrap_or(true);
    if require_mention && !msg.is_in_at_list {
        return PolicyDecision::reject(RejectReason::NoMention);
    }

    if let Some(ov) = ov {
        // Sender blocklist before allowlist within the group.
        if is_member(&ov.block_from, &msg.sender_id) {
            return PolicyDecision::reject(RejectReason::SenderBlocked);
        }
        if !ov.allow_from.is_empty() && !is_member(&ov.allow_from, &msg.sender_id) {
            return PolicyDecision::reject(RejectReason::SenderNotAllowed);
        }
    }

    // @all handling.
    let respond_to_mention_all = ov
        .and_then(|o| o.respond_to_mention_all)
        .or(cfg.respond_to_mention_all)
        .unwrap_or(false);
    if msg.mention_all && !respond_to_mention_all {
        return PolicyDecision::reject(RejectReason::MentionAll);
    }

    PolicyDecision::allow()
}

fn evaluate_dm(cfg: &PolicyConfig, msg: &IncomingMessage) -> PolicyDecision {
    match cfg.dm_mode() {
        "disabled" => PolicyDecision::reject(RejectReason::DmDisabled),
        "allowlist" => {
            if !is_member(&cfg.dm_allowlist, &msg.sender_id) {
                return PolicyDecision::reject(RejectReason::DmNotAllowed);
            }
            PolicyDecision::allow()
        }
        "blocklist" => {
            if is_member(&cfg.dm_blocklist, &msg.sender_id) {
                return PolicyDecision::reject(RejectReason::DmBlocked);
            }
            PolicyDecision::allow()
        }
        _ => PolicyDecision::allow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{GroupOverride, IncomingMessage};
    use std::collections::HashMap;

    fn msg(group: bool) -> IncomingMessage {
        IncomingMessage {
            conversation_type: if group { "group".into() } else { "dm".into() },
            sender_id: "u1".into(),
            sender_staff_id: "staff1".into(),
            ..Default::default()
        }
    }

    #[test]
    fn admin_bypasses_everything() {
        let cfg = PolicyConfig {
            admins: vec!["staff1".into()],
            ..Default::default()
        };
        let g = PolicyGate::new(cfg);
        let m = IncomingMessage {
            is_in_at_list: false,
            mention_all: true,
            ..msg(true)
        };
        assert!(g.evaluate(&m).allowed);
    }

    #[test]
    fn global_deny_wins_over_adminless_allow() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "cid".into(),
            GroupOverride {
                allow_from: vec!["u1".into()],
                ..Default::default()
            },
        );
        let cfg = PolicyConfig {
            deny_from: vec!["staff1".into()],
            allow_from: vec!["staff1".into()],
            group_overrides: overrides,
            require_mention: Some(false),
            ..Default::default()
        };
        let g = PolicyGate::new(cfg);
        assert_eq!(
            g.evaluate(&msg_group("cid")).reason,
            RejectReason::SenderDenied
        );
    }

    fn msg_group(cid: &str) -> IncomingMessage {
        IncomingMessage {
            conversation_id: cid.into(),
            is_in_at_list: true,
            ..msg(true)
        }
    }

    #[test]
    fn override_admits_under_global_allowlist() {
        let mut overrides = HashMap::new();
        overrides.insert("special".into(), GroupOverride::default());
        let cfg = PolicyConfig {
            group_allowlist: vec!["other".into()],
            group_overrides: overrides,
            require_mention: Some(false),
            ..Default::default()
        };
        let g = PolicyGate::new(cfg);
        assert!(g.evaluate(&msg_group("other")).allowed);
        assert_eq!(
            g.evaluate(&msg_group("random")).reason,
            RejectReason::GroupNotAllowed
        );
        assert!(g.evaluate(&msg_group("special")).allowed);
    }

    #[test]
    fn blocklist_not_exempt_by_override() {
        let mut overrides = HashMap::new();
        overrides.insert("blocked".into(), GroupOverride::default());
        let cfg = PolicyConfig {
            group_blocklist: vec!["blocked".into()],
            group_overrides: overrides,
            ..Default::default()
        };
        let g = PolicyGate::new(cfg);
        assert_eq!(
            g.evaluate(&msg_group("blocked")).reason,
            RejectReason::GroupBlocked
        );
    }

    #[test]
    fn dm_modes() {
        let g = PolicyGate::new(PolicyConfig {
            dm_mode: "disabled".into(),
            ..Default::default()
        });
        assert_eq!(g.evaluate(&msg(false)).reason, RejectReason::DmDisabled);

        let g = PolicyGate::new(PolicyConfig {
            dm_mode: "allowlist".into(),
            dm_allowlist: vec!["u9".into()],
            ..Default::default()
        });
        assert_eq!(g.evaluate(&msg(false)).reason, RejectReason::DmNotAllowed);

        let g = PolicyGate::new(PolicyConfig {
            dm_mode: "blocklist".into(),
            dm_blocklist: vec!["u1".into()],
            ..Default::default()
        });
        assert_eq!(g.evaluate(&msg(false)).reason, RejectReason::DmBlocked);

        let g = PolicyGate::new(PolicyConfig::default_extended());
        assert!(g.evaluate(&msg(false)).allowed);
    }
}
