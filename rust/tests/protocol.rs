use base64::{Engine, engine::general_purpose::STANDARD};
use paseo_relay::protocol::{MAX_CONTROL_BYTES, MAX_MESSAGE_BYTES, valid_handshake};

#[test]
fn matches_cowboy_payload_contract() {
    assert_eq!(MAX_MESSAGE_BYTES, 32 * 1024 * 1024 - 14);
    assert_eq!(MAX_CONTROL_BYTES, 64 * 1024);
}

#[test]
fn opaque_payloads_are_not_interpreted_as_handshakes() {
    for payload in [
        b"opaque".as_slice(),
        b"\xff",
        b"{}",
        br#"{"type":"other","key":null}"#,
    ] {
        assert!(valid_handshake(payload));
    }
}

#[test]
fn validates_both_handshake_names_and_canonical_x25519_coordinates() {
    let mut valid = [0u8; 32];
    valid[0] = 9;
    let mut prime = [255u8; 32];
    prime[0] = 237;
    prime[31] = 127;
    let mut high_bit = valid;
    high_bit[31] = 128;
    for kind in ["hello", "e2ee_hello"] {
        let encode = |key: serde_json::Value| {
            serde_json::to_vec(&serde_json::json!({"type":kind,"key":key})).unwrap()
        };
        assert!(valid_handshake(&encode(STANDARD.encode(valid).into())));
        for key in [
            vec![],
            vec![0; 32],
            vec![1],
            prime.to_vec(),
            high_bit.to_vec(),
        ] {
            assert!(!valid_handshake(&encode(STANDARD.encode(key).into())));
        }
        assert!(!valid_handshake(&encode(serde_json::Value::Null)));
        assert!(!valid_handshake(&encode(
            STANDARD.encode(valid).trim_end_matches('=').into()
        )));
    }
}

#[test]
fn rejects_all_elixir_low_order_keys() {
    for hex in [
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0100000000000000000000000000000000000000000000000000000000000000",
        "e0eb7a7c3b41b8ae1656e3faf19fc46ada098deb9c32b1fd866205165f49b800",
        "5f9c95bca3508c24b1d0b1559c83ef5b04445cc4581c8e86d8224eddd09f1157",
        "ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
        "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
        "eeffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
    ] {
        let key: Vec<u8> = (0..64)
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let payload =
            serde_json::to_vec(&serde_json::json!({"type":"hello","key":STANDARD.encode(key)}))
                .unwrap();
        assert!(!valid_handshake(&payload), "accepted {hex}");
    }
}

#[test]
fn classification_preserves_last_duplicate_fields_and_json_string_escapes() {
    use paseo_relay::protocol::{control_ping, handshake};
    let key = STANDARD.encode({
        let mut key = [0u8; 32];
        key[0] = 9;
        key
    });
    let escaped = key
        .chars()
        .map(|c| format!("\\u{:04x}", c as u32))
        .collect::<String>();
    let payload = format!(
        r#"{{"type":"other","type":"\u0068ello","key":null,"key":"{escaped}","ignored":[{{"type":"hello","key":false}}]}}"#
    );
    let parsed = handshake(payload.as_bytes()).unwrap();
    assert_eq!(parsed.kind, "hello");
    assert!(parsed.accepted);
    assert!(!valid_handshake(
        format!(r#"{{"type":"hello","key":"{key}","key":null}}"#).as_bytes()
    ));
    assert!(handshake(br#"{"type":"hello","key":"bad","broken":}"#).is_none());
    assert!(control_ping(
        br#"{"type":"other","type":"ping","padding":[1,2,3]}"#
    ));
    assert!(!control_ping(br#"{"type":"ping","type":null}"#));
}
