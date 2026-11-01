use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub cluster: Option<ClusterConfig>,
    pub minimum_cluster_size: usize,
    pub ownership_target: String,
    pub reroute_header: axum::http::HeaderName,
    pub listen: SocketAddr,
    pub http_idle_timeout: Duration,
    pub tcp_receive_buffer_bytes: usize,
    pub memory_watermark_bytes: usize,
    pub max_connections: usize,
    pub ingress_budget_bytes: usize,
    pub ingress_weight: usize,
    pub delivery_timeout: Duration,
    pub data_attach_timeout: Duration,
    pub control_queue_bytes: usize,
    pub drain: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cluster: None,
            minimum_cluster_size: 1,
            ownership_target: "local".into(),
            reroute_header: axum::http::HeaderName::from_static("x-reroute-target"),
            listen: "127.0.0.1:4000".parse().unwrap(),
            http_idle_timeout: Duration::from_secs(15),
            tcp_receive_buffer_bytes: 65536,
            memory_watermark_bytes: 0,
            max_connections: 20_000,
            ingress_budget_bytes: 512 * 1024 * 1024,
            ingress_weight: 4,
            delivery_timeout: Duration::from_secs(30),
            data_attach_timeout: Duration::from_secs(15),
            control_queue_bytes: 1024 * 1024,
            drain: false,
        }
    }
}

impl Config {
    pub fn load(environment: &HashMap<String, String>) -> Result<Self, String> {
        let get = |key: &str| environment.get(key).map(String::as_str);
        let number = |key: &str, default: usize, min: usize, max: usize| -> Result<usize, String> {
            let value = match get(key) {
                None => default,
                Some(value) => value
                    .parse()
                    .map_err(|_| format!("{key} must be an integer between {min} and {max}"))?,
            };
            if (min..=max).contains(&value) {
                Ok(value)
            } else {
                Err(format!("{key} must be an integer between {min} and {max}"))
            }
        };
        let minimum_cluster_size = number("PASEO_RELAY_MIN_CLUSTER_SIZE", 1, 1, 1000)?;
        let ownership_target = get("PASEO_RELAY_OWNERSHIP_TARGET")
            .unwrap_or("local")
            .to_owned();
        if ownership_target.is_empty()
            || ownership_target.len() > 1024
            || axum::http::HeaderValue::from_str(&ownership_target).is_err()
        {
            return Err("PASEO_RELAY_OWNERSHIP_TARGET must be a nonempty HTTP header value up to 1024 bytes".into());
        }
        let reroute_header = get("PASEO_RELAY_REROUTE_HEADER")
            .unwrap_or("x-reroute-target")
            .parse()
            .map_err(|_| "PASEO_RELAY_REROUTE_HEADER must be an HTTP header name")?;
        let peers = get("PASEO_RELAY_CLUSTER_NODES")
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>();
        if peers.len() > 1000 {
            return Err("PASEO_RELAY_CLUSTER_NODES supports at most 1000 nodes".into());
        }
        for peer in &peers {
            node_address(peer)?;
        }
        let query = get("PASEO_RELAY_CLUSTER_QUERY")
            .filter(|q| !q.is_empty())
            .map(str::to_owned);
        if let Some(query) = &query {
            node_address(&format!("dns@{query}"))?;
        }
        let cluster = if !peers.is_empty()
            || query.is_some()
            || get("PASEO_RELAY_CLUSTER_LISTEN").is_some()
        {
            let node = get("RELEASE_NODE")
                .ok_or("RELEASE_NODE is required for a Rust cluster")?
                .to_owned();
            node_address(&node)?;
            let cookie =
                get("RELEASE_COOKIE").ok_or("RELEASE_COOKIE is required for a Rust cluster")?;
            if cookie.len() < 32 || cookie.len() > 256 {
                return Err(
                    "RELEASE_COOKIE must contain 32 to 256 bytes for a Rust cluster".into(),
                );
            }
            if ownership_target == "local" {
                return Err("PASEO_RELAY_OWNERSHIP_TARGET must identify this cluster node".into());
            }
            let listen = get("PASEO_RELAY_CLUSTER_LISTEN")
                .unwrap_or("127.0.0.1:4400")
                .parse::<SocketAddr>()
                .map_err(|_| "PASEO_RELAY_CLUSTER_LISTEN must be an IP:port socket address")?;
            if listen.port() == 0 {
                return Err("PASEO_RELAY_CLUSTER_LISTEN port must be nonzero".into());
            }
            Some(ClusterConfig {
                node,
                listen,
                peers: peers.into_iter().collect(),
                query,
                cookie: cookie.as_bytes().to_vec(),
            })
        } else {
            None
        };
        let memory_watermark_bytes = number(
            "PASEO_RELAY_MEMORY_WATERMARK_BYTES",
            0,
            0,
            64 * 1024 * 1024 * 1024,
        )?;
        if memory_watermark_bytes != 0 && memory_watermark_bytes < 256 * 1024 * 1024 {
            return Err(
                "PASEO_RELAY_MEMORY_WATERMARK_BYTES must be zero or at least 268435456".into(),
            );
        }
        let host: IpAddr = get("PASEO_RELAY_HOST")
            .unwrap_or("127.0.0.1")
            .parse()
            .map_err(|_| "PASEO_RELAY_HOST must be an IP address")?;
        let port = number("PASEO_RELAY_PORT", 4000, 1, 65535)? as u16;
        let drain = match get("PASEO_RELAY_DRAIN").unwrap_or("false") {
            "true" => true,
            "false" => false,
            _ => return Err("PASEO_RELAY_DRAIN must be true or false".into()),
        };
        let max_connections = number("PASEO_RELAY_ACCEPTORS", 100, 1, 1000)?
            * number("PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", 200, 1, 1_000_000)?;
        let ingress_budget_bytes = number(
            "PASEO_RELAY_INGRESS_BUDGET_BYTES",
            512 * 1024 * 1024,
            128 * 1024 * 1024,
            8 * 1024 * 1024 * 1024,
        )?;
        let ingress_weight = number("PASEO_RELAY_INGRESS_WEIGHT", 4, 1, 16)?;
        if ingress_budget_bytes < crate::protocol::MAX_MESSAGE_BYTES * ingress_weight {
            return Err("PASEO_RELAY_INGRESS_BUDGET_BYTES must admit one maximum assembled message at the configured weight".into());
        }
        let delivery_ms = number("PASEO_RELAY_DELIVERY_TIMEOUT_MS", 30000, 100, 120000)?;
        if delivery_ms >= number("PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS", 35000, 100, 300000)? {
            return Err("PASEO_RELAY_DELIVERY_TIMEOUT_MS must be lower than PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS".into());
        }
        Ok(Self {
            cluster,
            minimum_cluster_size,
            ownership_target,
            reroute_header,
            listen: SocketAddr::new(host, port),
            http_idle_timeout: Duration::from_millis(number(
                "PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS",
                15000,
                100,
                120000,
            )? as u64),
            tcp_receive_buffer_bytes: number(
                "PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES",
                65536,
                4096,
                1024 * 1024,
            )?,
            memory_watermark_bytes,
            drain,
            max_connections,
            ingress_budget_bytes,
            ingress_weight,
            delivery_timeout: Duration::from_millis(delivery_ms as u64),
            data_attach_timeout: Duration::from_millis(number(
                "PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS",
                15000,
                1000,
                120000,
            )? as u64),
            control_queue_bytes: number(
                "PASEO_RELAY_CONTROL_QUEUE_BYTES",
                1024 * 1024,
                64,
                64 * 1024 * 1024,
            )?,
        })
    }
}

#[derive(Clone)]
pub struct ClusterConfig {
    pub(crate) node: String,
    pub(crate) listen: SocketAddr,
    pub(crate) peers: Vec<String>,
    pub(crate) query: Option<String>,
    pub(crate) cookie: Vec<u8>,
}

impl std::fmt::Debug for ClusterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterConfig")
            .field("node", &self.node)
            .field("listen", &self.listen)
            .field("peers", &self.peers)
            .field("query", &self.query)
            .finish_non_exhaustive()
    }
}

pub(crate) fn node_address(node: &str) -> Result<String, String> {
    let error =
        || "cluster nodes must use name@host or name@host:port (at most 255 bytes)".to_owned();
    let (name, host) = node.split_once('@').ok_or_else(error)?;
    if node.len() > 255
        || name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(error());
    }
    if let Ok(address) = host.parse::<SocketAddr>() {
        return if address.port() == 0 {
            Err(error())
        } else {
            Ok(address.to_string())
        };
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, 4400).to_string());
    }
    let (host, port) = match host.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>()
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(error)?,
        ),
        None => (host, 4400),
    };
    if host.is_empty()
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(error());
    }
    Ok(format!("{host}:{port}"))
}
