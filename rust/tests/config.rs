use paseo_relay::Config;
use std::collections::HashMap;

fn load(entries: &[(&str, &str)]) -> Result<Config, String> {
    Config::load(
        &entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
    )
}

#[test]
fn default_and_supported_linux_environment_match_original_contract() {
    let default = load(&[]).unwrap();
    assert_eq!(default.listen.to_string(), "127.0.0.1:4000");
    assert_eq!(default.max_connections, 20_000);
    let config = load(&[
        ("PASEO_RELAY_HOST", "::1"),
        ("PASEO_RELAY_PORT", "9876"),
        ("PASEO_RELAY_ACCEPTORS", "2"),
        ("PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", "3"),
        ("PASEO_RELAY_DRAIN", "true"),
    ])
    .unwrap();
    assert_eq!(config.listen.to_string(), "[::1]:9876");
    assert_eq!(config.max_connections, 6);
    assert!(config.drain);
}

#[test]
fn rejects_invalid_values_and_unsafe_budget_and_timeout_combinations() {
    for entries in [
        vec![("PASEO_RELAY_HOST", "localhost")],
        vec![("PASEO_RELAY_PORT", "0")],
        vec![("PASEO_RELAY_DRAIN", "1")],
        vec![("PASEO_RELAY_ACCEPTORS", "0")],
        vec![("PASEO_RELAY_INGRESS_WEIGHT", "17")],
        vec![
            ("PASEO_RELAY_INGRESS_BUDGET_BYTES", "134217728"),
            ("PASEO_RELAY_INGRESS_WEIGHT", "16"),
        ],
        vec![("PASEO_RELAY_DELIVERY_TIMEOUT_MS", "35000")],
    ] {
        assert!(load(&entries).is_err(), "accepted {entries:?}");
    }
}

#[test]
fn unsupported_distributed_settings_cannot_silently_start_a_local_relay() {
    for key in ["PASEO_RELAY_CLUSTER_QUERY", "PASEO_RELAY_CLUSTER_NODES"] {
        assert!(load(&[(key, "peer")]).is_err());
    }
    assert!(load(&[("PASEO_RELAY_MIN_CLUSTER_SIZE", "2")]).is_err());
}
