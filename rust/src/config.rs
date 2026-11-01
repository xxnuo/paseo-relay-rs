use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
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
            listen: "127.0.0.1:4000".parse().unwrap(),
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
        for key in ["PASEO_RELAY_CLUSTER_QUERY", "PASEO_RELAY_CLUSTER_NODES"] {
            if get(key).is_some_and(|v| !v.is_empty()) {
                return Err(format!(
                    "{key}: distributed ownership is not implemented in Rust yet"
                ));
            }
        }
        if number("PASEO_RELAY_MIN_CLUSTER_SIZE", 1, 1, 1000)? != 1 {
            return Err("distributed ownership is not implemented in Rust yet".into());
        }
        if number(
            "PASEO_RELAY_MEMORY_WATERMARK_BYTES",
            0,
            0,
            64 * 1024 * 1024 * 1024,
        )? != 0
        {
            return Err("Rust memory watermark is not implemented yet".into());
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
            listen: SocketAddr::new(host, port),
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
