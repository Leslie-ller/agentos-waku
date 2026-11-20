//! Desktop ownership of the Waku daemon process.

use std::net::Ipv4Addr;
use std::path::PathBuf;

use anyhow::{Context as _, anyhow, bail};

/// Classification of a reachable address on this host for multi-device pairing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonEndpointKind {
    Lan,
    Tailscale,
    Hostname,
}

/// One network host that mobile/web/desktop clients can use to reach this daemon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonNetworkEndpoint {
    pub kind: DaemonEndpointKind,
    pub host: String,
    pub interface_name: Option<String>,
}

impl DaemonNetworkEndpoint {
    pub fn web_url(&self, port: u16) -> String {
        format!("http://{}:{port}", self.host)
    }

    pub fn websocket_url(&self, port: u16) -> String {
        format!("ws://{}:{port}", self.host)
    }

    pub fn pairing_url(&self, port: u16, token: &str) -> String {
        format!("http://{}:{port}/#token={token}", self.host)
    }
}

/// Classify a non-loopback IPv4 address for mobile/remote pairing.
///
/// Tailscale assigns addresses from the RFC 6598 CGNAT range (`100.64.0.0/10`),
/// while private LAN interfaces use RFC 1918 (`10/8`, `172.16/12`, `192.168/16`).
/// Loopback (`127/8`), link-local (`169.254/16`), multicast, and unspecified
/// addresses are ignored.
pub fn classify_ipv4_endpoint(ip: Ipv4Addr) -> Option<DaemonEndpointKind> {
    if ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
    {
        return None;
    }
    let [a, b, _, _] = ip.octets();
    if a == 100 && (64..=127).contains(&b) {
        return Some(DaemonEndpointKind::Tailscale);
    }
    if ip.is_private() {
        return Some(DaemonEndpointKind::Lan);
    }
    Some(DaemonEndpointKind::Lan)
}

/// Discover reachable LAN, Tailscale, and hostname endpoints once at startup.
/// Results are sorted LAN first, then Tailscale, then the mDNS/local hostname,
/// with duplicates removed so Settings → Daemon can offer one-tap QR pairing.
pub fn local_network_endpoints(fallback_hostname: &str) -> Vec<DaemonNetworkEndpoint> {
    let mut lan = Vec::new();
    let mut tailscale = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for (ip, iface) in enumerate_local_ipv4() {
        let Some(kind) = classify_ipv4_endpoint(ip) else {
            continue;
        };
        let host = ip.to_string();
        if !seen.insert(host.clone()) {
            continue;
        }
        let endpoint = DaemonNetworkEndpoint {
            kind,
            host,
            interface_name: iface,
        };
        match kind {
            DaemonEndpointKind::Lan => lan.push(endpoint),
            DaemonEndpointKind::Tailscale => tailscale.push(endpoint),
            DaemonEndpointKind::Hostname => {}
        }
    }

    let mut endpoints = Vec::new();
    endpoints.extend(lan);
    endpoints.extend(tailscale);

    let host = fallback_hostname.trim();
    if !host.is_empty() && seen.insert(host.to_owned()) {
        endpoints.push(DaemonNetworkEndpoint {
            kind: DaemonEndpointKind::Hostname,
            host: host.to_owned(),
            interface_name: None,
        });
    }
    endpoints
}

#[cfg(unix)]
fn enumerate_local_ipv4() -> Vec<(Ipv4Addr, Option<String>)> {
    let mut results = Vec::new();
    let mut addrs: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut addrs) } != 0 || addrs.is_null() {
        return results;
    }
    let mut cursor = addrs;
    while !cursor.is_null() {
        let ifa = unsafe { &*cursor };
        cursor = ifa.ifa_next;
        let is_up = (ifa.ifa_flags as i32 & libc::IFF_UP) != 0;
        let is_loopback = (ifa.ifa_flags as i32 & libc::IFF_LOOPBACK) != 0;
        if !is_up || is_loopback || ifa.ifa_addr.is_null() {
            continue;
        }
        let family = unsafe { (*ifa.ifa_addr).sa_family as i32 };
        if family != libc::AF_INET {
            continue;
        }
        let sin = unsafe { &*(ifa.ifa_addr as *const libc::sockaddr_in) };
        let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
        let name = if ifa.ifa_name.is_null() {
            None
        } else {
            Some(
                unsafe { std::ffi::CStr::from_ptr(ifa.ifa_name) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        // Skip common virtual container bridges unless nothing else exists.
        if let Some(ref iface) = name {
            if iface.starts_with("bridge") || iface.starts_with("docker") || iface.starts_with("veth") {
                continue;
            }
        }
        results.push((ip, name));
    }
    unsafe { libc::freeifaddrs(addrs) };
    results
}

#[cfg(not(unix))]
fn enumerate_local_ipv4() -> Vec<(Ipv4Addr, Option<String>)> {
    let mut results = Vec::new();
    // Binding an unbound UDP socket and connecting to a well-known external IP
    // does not transmit packets; the OS routing table picks the active LAN IP.
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0")
        && socket.connect("8.8.8.8:80").is_ok()
        && let Ok(std::net::SocketAddr::V4(addr)) = socket.local_addr()
    {
        results.push((*addr.ip(), None));
    }
    results
}

pub fn start_process() -> anyhow::Result<waku_client::DaemonSupervisor> {
    let address = std::env::var(waku_client::DAEMON_ADDRESS_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    let token = std::env::var(waku_client::DAEMON_TOKEN_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    match (address, token) {
        (Some(address), Some(token)) => {
            return waku_client::DaemonSupervisor::connect(address.trim(), token);
        }
        (Some(_), None) => bail!(
            "{} is set but {} is missing",
            waku_client::DAEMON_ADDRESS_ENV,
            waku_client::DAEMON_TOKEN_ENV
        ),
        (None, Some(_)) => bail!(
            "{} is set but {} is missing",
            waku_client::DAEMON_TOKEN_ENV,
            waku_client::DAEMON_ADDRESS_ENV
        ),
        (None, None) => {}
    }
    let app_settings = waku_client::persistence::load_or_create_app_settings()
        .context("could not load desktop daemon settings")?;
    waku_client::DaemonSupervisor::spawn_configured(
        &daemon_executable_path()?,
        cfg!(debug_assertions),
        app_settings.daemon_exposure,
    )
}

/// Resolve the local host name once during app construction. Settings can
/// then show a useful LAN URL without touching the OS from a render frame.
pub fn local_hostname() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buffer = [0_u8; 256];
        let result = unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) };
        if result == 0 {
            let length = buffer
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(buffer.len());
            let hostname = String::from_utf8_lossy(&buffer[..length]).trim().to_owned();
            if !hostname.is_empty() {
                return Some(hostname);
            }
        }
    }
    // `COMPUTERNAME` is the Windows equivalent and is always set; `HOSTNAME`
    // covers the shells that export it.
    ["COMPUTERNAME", "HOSTNAME"]
        .into_iter()
        .filter_map(|name| std::env::var(name).ok())
        .map(|hostname| hostname.trim().to_owned())
        .find(|hostname| !hostname.is_empty())
}

fn daemon_executable_path() -> anyhow::Result<PathBuf> {
    if let Some(path) = std::env::var_os("WAKU_DAEMON_PATH").filter(|path| !path.is_empty()) {
        return Ok(path.into());
    }
    let executable = format!("waku-daemon{}", std::env::consts::EXE_SUFFIX);
    let current = std::env::current_exe().context("could not locate the Waku executable")?;

    // Development keeps the daemon beside Cargo's debug artifacts rather than
    // inside Waku Debug.app. The supervisor watches this file and swaps only
    // the daemon when the development watcher relinks it.
    #[cfg(debug_assertions)]
    if let Some(debug_directory) = current
        .ancestors()
        .find(|candidate| candidate.file_name().is_some_and(|name| name == "debug"))
    {
        let external = debug_directory.join(&executable);
        if external.is_file() {
            return Ok(external);
        }
    }

    let sibling = current
        .parent()
        .map(|directory| directory.join(&executable))
        .ok_or_else(|| anyhow!("Waku executable has no parent directory"))?;
    if sibling.is_file() {
        return Ok(sibling);
    }
    #[cfg(debug_assertions)]
    bail!(
        "Waku daemon was not found in Cargo's debug directory or next to the app executable: {}",
        sibling.display(),
    );
    #[cfg(not(debug_assertions))]
    bail!(
        "Waku daemon is missing next to the app executable: {}",
        sibling.display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_lan_tailscale_and_ignored_ipv4_addresses() {
        assert_eq!(
            classify_ipv4_endpoint(Ipv4Addr::new(192, 168, 1, 192)),
            Some(DaemonEndpointKind::Lan)
        );
        assert_eq!(
            classify_ipv4_endpoint(Ipv4Addr::new(10, 0, 0, 5)),
            Some(DaemonEndpointKind::Lan)
        );
        assert_eq!(
            classify_ipv4_endpoint(Ipv4Addr::new(100, 84, 159, 56)),
            Some(DaemonEndpointKind::Tailscale)
        );
        assert_eq!(
            classify_ipv4_endpoint(Ipv4Addr::new(127, 0, 0, 1)),
            None
        );
        assert_eq!(
            classify_ipv4_endpoint(Ipv4Addr::new(169, 254, 10, 2)),
            None
        );
    }

    #[test]
    fn formats_endpoint_urls_consistently() {
        let ep = DaemonNetworkEndpoint {
            kind: DaemonEndpointKind::Tailscale,
            host: "100.84.159.56".into(),
            interface_name: Some("utun4".into()),
        };
        assert_eq!(ep.web_url(34123), "http://100.84.159.56:34123");
        assert_eq!(ep.websocket_url(34123), "ws://100.84.159.56:34123");
        assert_eq!(
            ep.pairing_url(34123, "abc123"),
            "http://100.84.159.56:34123/#token=abc123"
        );
    }
}
