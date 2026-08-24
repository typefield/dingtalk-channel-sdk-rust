//! SSRF guard: block fetches into private/loopback/link-local networks
//! (Go `safety/ssrf_guard.go` port).

use crate::error::{Error, ErrorCode, Result};

const BLOCKED_V4: [(&str, u8); 14] = [
    ("0.0.0.0", 8),
    ("10.0.0.0", 8),
    ("127.0.0.0", 8),
    ("169.254.0.0", 16),
    ("172.16.0.0", 12),
    ("192.168.0.0", 16),
    ("100.64.0.0", 10),
    ("192.0.0.0", 24),
    ("192.0.2.0", 24),
    ("198.18.0.0", 15),
    ("198.51.100.0", 24),
    ("203.0.113.0", 24),
    ("224.0.0.0", 4),
    ("240.0.0.0", 4),
];

fn ipv4_blocked(ip: std::net::Ipv4Addr) -> bool {
    for (base, prefix) in BLOCKED_V4 {
        if let Ok(net) = format!("{base}/{prefix}").parse::<ipnet_like::V4Net>() {
            if net.contains(ip) {
                return true;
            }
        }
    }
    false
}

/// Minimal CIDR matcher (avoids pulling an extra dependency).
mod ipnet_like {
    use std::net::Ipv4Addr;

    pub struct V4Net {
        pub addr: Ipv4Addr,
        pub prefix: u8,
    }

    impl V4Net {
        pub fn contains(&self, ip: Ipv4Addr) -> bool {
            let a = u32::from(self.addr);
            let b = u32::from(ip);
            let mask = if self.prefix == 0 {
                0
            } else {
                u32::MAX << (32 - self.prefix)
            };
            a & mask == b & mask
        }
    }

    impl std::str::FromStr for V4Net {
        type Err = String;
        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let (addr, prefix) = s
                .split_once('/')
                .ok_or_else(|| "missing prefix".to_string())?;
            let addr: Ipv4Addr = addr
                .parse()
                .map_err(|e: std::net::AddrParseError| e.to_string())?;
            let prefix: u8 = prefix
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            Ok(V4Net { addr, prefix })
        }
    }
}

fn ipv6_blocked(ip: std::net::Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    let unique_local = first & 0xfe00 == 0xfc00;
    let unicast_link_local = first & 0xffc0 == 0xfe80;
    if ip.is_loopback()
        || ip.is_unspecified()
        || unique_local
        || unicast_link_local
        || ip.is_multicast()
    {
        return true;
    }
    let s = ip.to_string();
    s.starts_with("fe80:") || s.starts_with("fc") || s.starts_with("fd") || s.starts_with("ff")
}

/// Whitelist match: exact host or `*.suffix`.
pub fn host_allowed(host: &str, allowlist: &[String]) -> bool {
    if allowlist.is_empty() || host.is_empty() {
        return false;
    }
    let h = host.trim_end_matches('.').to_lowercase();
    for entry in allowlist {
        let e = entry.trim_end_matches('.').to_lowercase();
        if h == e {
            return true;
        }
        if let Some(suffix) = e.strip_prefix("*.") {
            if h.ends_with(&suffix) {
                return true;
            }
        }
    }
    false
}

fn blocked(code_msg: String) -> Error {
    Error::Classified {
        code: ErrorCode::SsrfBlocked,
        message: code_msg,
    }
}

/// Validate a URL is publicly reachable before fetching.
pub async fn assert_public_url(u: &str) -> Result<()> {
    assert_public_url_with_allowlist(u, &[]).await
}

/// Public-URL check with hostname allowlist exemption (enterprise CDNs).
pub async fn assert_public_url_with_allowlist(u: &str, allowlist: &[String]) -> Result<()> {
    let parsed = url::parse(u).map_err(|_| blocked("invalid url".into()))?;
    if parsed.scheme != "http" && parsed.scheme != "https" {
        return Err(blocked(format!("protocol {}", parsed.scheme)));
    }
    let host = parsed.host.clone();
    if host_allowed(&host, allowlist) {
        return Ok(());
    }

    // Direct IP literal?
    if let Ok(v4) = host.parse::<std::net::Ipv4Addr>() {
        if ipv4_blocked(v4) {
            return Err(blocked(v4.to_string()));
        }
        return Ok(());
    }
    if let Ok(v6) = host.parse::<std::net::Ipv6Addr>() {
        if ipv6_blocked(v6) {
            return Err(blocked(v6.to_string()));
        }
        return Ok(());
    }

    // DNS resolve then check every address.
    let addrs = tokio::net::lookup_host((host.as_str(), 443))
        .await
        .map_err(|e| blocked(format!("dns lookup failed: {e}")))?;
    for addr in addrs {
        match addr.ip() {
            std::net::IpAddr::V4(v4) => {
                if ipv4_blocked(v4) {
                    return Err(blocked(v4.to_string()));
                }
            }
            std::net::IpAddr::V6(v6) => {
                if ipv6_blocked(v6) {
                    return Err(blocked(v6.to_string()));
                }
            }
        }
    }
    Ok(())
}

mod url {
    #[derive(Debug)]
    pub struct ParsedUrl {
        pub scheme: String,
        pub host: String,
    }

    /// Minimal URL splitter good enough for http(s) URLs.
    pub fn parse(u: &str) -> Result<ParsedUrl, ()> {
        let (scheme, rest) = u.split_once("://").ok_or(())?;
        if scheme != "http" && scheme != "https" {
            return Err(());
        }
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        // strip userinfo
        let hostport = authority.rsplit('@').next().unwrap_or(authority);
        let host = if let Some(h) = hostport.strip_prefix('[') {
            h.split(']').next().unwrap_or(h).to_string()
        } else {
            hostport.split(':').next().unwrap_or(hostport).to_string()
        };
        if host.is_empty() {
            return Err(());
        }
        Ok(ParsedUrl {
            scheme: scheme.to_string(),
            host,
        })
    }
}
