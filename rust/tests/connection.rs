use paseo_relay::connection::{Connection, Role};
use std::collections::HashMap;

fn parse(entries: &[(&str, &str)]) -> Result<Connection, &'static str> {
    Connection::from_query(
        &entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
    )
}

#[test]
fn follows_original_query_contract() {
    let v1 = parse(&[
        ("role", "server"),
        ("serverId", "room"),
        ("connectionId", "ignored"),
    ])
    .unwrap();
    assert_eq!(v1.role, Role::Server);
    assert_eq!(v1.version, 1);
    assert_eq!(v1.connection_id, None);
    let control = parse(&[("role", "server"), ("serverId", "room"), ("v", " 2 ")]).unwrap();
    assert_eq!(control.connection_id.as_deref(), Some(""));
    let client = parse(&[("role", "client"), ("serverId", "room"), ("v", "2")]).unwrap();
    let generated = client.connection_id.unwrap();
    assert!(generated.starts_with("conn_"));
    assert_eq!(generated.len(), 21);
    assert!(parse(&[("role", "client"), ("serverId", "room"), ("v", "3")]).is_err());
    assert!(parse(&[("role", "unknown"), ("serverId", "room")]).is_err());
    assert!(parse(&[("role", "server")]).is_err());
}

#[test]
fn route_limits_count_utf8_bytes_and_trim_only_connection_id() {
    let limit = "é".repeat(128);
    assert!(parse(&[("role", "client"), ("serverId", &limit)]).is_ok());
    assert_eq!(
        parse(&[("role", "client"), ("serverId", &(limit.clone() + "x"))]).unwrap_err(),
        "serverId is too long"
    );
    assert!(
        parse(&[
            ("role", "server"),
            ("serverId", " "),
            ("v", "2"),
            ("connectionId", &format!(" {limit} "))
        ])
        .is_ok()
    );
    assert_eq!(
        parse(&[
            ("role", "server"),
            ("serverId", "room"),
            ("v", "2"),
            ("connectionId", &(limit + "x"))
        ])
        .unwrap_err(),
        "connectionId is too long"
    );
}
