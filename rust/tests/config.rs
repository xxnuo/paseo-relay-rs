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
        vec![("PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS", "0")],
        vec![("PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", "1")],
        vec![("PASEO_RELAY_MEMORY_WATERMARK_BYTES", "1")],
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
fn distributed_settings_require_authenticated_node_configuration() {
    for key in ["PASEO_RELAY_CLUSTER_QUERY", "PASEO_RELAY_CLUSTER_NODES"] {
        assert!(load(&[(key, "peer")]).is_err());
    }
    assert_eq!(
        load(&[("PASEO_RELAY_MIN_CLUSTER_SIZE", "2")])
            .unwrap()
            .minimum_cluster_size,
        2
    );
}

#[test]
fn linux_runtime_protection_settings_are_loaded() {
    let config = load(&[
        ("PASEO_RELAY_MEMORY_WATERMARK_BYTES", "268435456"),
        ("PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS", "200"),
        ("PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES", "131072"),
    ])
    .unwrap();
    assert_eq!(config.memory_watermark_bytes, 268435456);
    assert_eq!(config.http_idle_timeout.as_millis(), 200);
    assert_eq!(config.tcp_receive_buffer_bytes, 131072);
}

#[test]
fn cluster_environment_validates_identity_authentication_and_http_headers() {
    let environment = [
        (
            "PASEO_RELAY_CLUSTER_NODES",
            "a@host.example:4401,b@[::1]:4402",
        ),
        ("PASEO_RELAY_CLUSTER_LISTEN", "127.0.0.1:4400"),
        ("RELEASE_NODE", "self@host.example"),
        ("RELEASE_COOKIE", "test-only-cookie-at-least-32-bytes"),
        ("PASEO_RELAY_OWNERSHIP_TARGET", "opaque-self"),
        ("PASEO_RELAY_REROUTE_HEADER", "x-owner"),
    ];
    let config = load(&environment).unwrap();
    assert!(config.cluster.is_some());
    assert_eq!(config.reroute_header, "x-owner");
    assert!(!format!("{config:?}").contains("test-only-cookie-at-least-32-bytes"));
    for (key, value) in [
        ("RELEASE_NODE", "bad identity"),
        ("RELEASE_COOKIE", "short"),
        ("PASEO_RELAY_CLUSTER_NODES", "node@host:0"),
        ("PASEO_RELAY_CLUSTER_LISTEN", "127.0.0.1:0"),
        ("PASEO_RELAY_OWNERSHIP_TARGET", "bad\r\nheader"),
        ("PASEO_RELAY_REROUTE_HEADER", "bad header"),
    ] {
        let mut invalid = environment.to_vec();
        invalid.iter_mut().find(|entry| entry.0 == key).unwrap().1 = value;
        assert!(load(&invalid).is_err(), "accepted {key}");
    }
}

#[test]
fn tls_requires_both_nonempty_certificate_and_key_paths() {
    for entries in [
        vec![("PASEO_RELAY_TLS_CERT_FILE", "/cert.pem")],
        vec![("PASEO_RELAY_TLS_KEY_FILE", "/key.pem")],
        vec![
            ("PASEO_RELAY_TLS_CERT_FILE", ""),
            ("PASEO_RELAY_TLS_KEY_FILE", "/key.pem"),
        ],
        vec![
            ("PASEO_RELAY_TLS_CERT_FILE", "/cert.pem"),
            ("PASEO_RELAY_TLS_KEY_FILE", ""),
        ],
    ] {
        assert!(load(&entries).is_err(), "accepted {entries:?}");
    }
}
