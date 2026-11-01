use base64::{Engine, engine::general_purpose::STANDARD};

pub const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024 - 14;
pub const MAX_CONTROL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub kind: &'static str,
    pub accepted: bool,
}

// Keep aligned with lib/paseo_relay/handshake_validation.ex; payloads otherwise stay opaque.
pub fn valid_handshake(payload: &[u8]) -> bool {
    handshake(payload).is_none_or(|value| value.accepted)
}

pub fn handshake(payload: &[u8]) -> Option<Handshake> {
    let fields = serde_json::from_slice::<Fields<'_>>(payload).ok()?;
    let kind = match small_string(fields.kind?, 10 * 6 + 2)?.as_str() {
        "hello" => "hello",
        "e2ee_hello" => "e2ee_hello",
        _ => return None,
    };
    let accepted = fields
        .key
        .and_then(|key| small_string(key, 44 * 6 + 2))
        .is_some_and(|key| valid_key(&key));
    Some(Handshake { kind, accepted })
}

pub fn control_ping(payload: &[u8]) -> bool {
    serde_json::from_slice::<Fields<'_>>(payload)
        .ok()
        .and_then(|fields| fields.kind)
        .and_then(|kind| small_string(kind, 10 * 6 + 2))
        .is_some_and(|kind| kind == "ping")
}

// JSON may escape each ASCII character as six bytes.
fn small_string(raw: &serde_json::value::RawValue, limit: usize) -> Option<String> {
    (raw.get().len() <= limit)
        .then(|| serde_json::from_str(raw.get()).ok())
        .flatten()
}

#[derive(Default)]
struct Fields<'a> {
    kind: Option<&'a serde_json::value::RawValue>,
    key: Option<&'a serde_json::value::RawValue>,
}

impl<'de> serde::Deserialize<'de> for Fields<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Fields<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut fields = Fields::default();
                while let Some(name) = map.next_key::<String>()? {
                    let value = map.next_value::<&serde_json::value::RawValue>()?;
                    match name.as_str() {
                        "type" => fields.kind = Some(value),
                        "key" => fields.key = Some(value),
                        _ => {}
                    }
                }
                Ok(fields)
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

fn valid_key(encoded: &str) -> bool {
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(key) = <[u8; 32]>::try_from(decoded) else {
        return false;
    };
    if STANDARD.encode(key) != encoded {
        return false;
    }
    let mut prime = [255u8; 32];
    prime[0] = 237;
    prime[31] = 127;
    if key.iter().rev().cmp(prime.iter().rev()) != std::cmp::Ordering::Less {
        return false;
    }
    let low_order = [
        [0u8; 32],
        {
            let mut k = [0u8; 32];
            k[0] = 1;
            k
        },
        [
            0xe0, 0xeb, 0x7a, 0x7c, 0x3b, 0x41, 0xb8, 0xae, 0x16, 0x56, 0xe3, 0xfa, 0xf1, 0x9f,
            0xc4, 0x6a, 0xda, 0x09, 0x8d, 0xeb, 0x9c, 0x32, 0xb1, 0xfd, 0x86, 0x62, 0x05, 0x16,
            0x5f, 0x49, 0xb8, 0x00,
        ],
        [
            0x5f, 0x9c, 0x95, 0xbc, 0xa3, 0x50, 0x8c, 0x24, 0xb1, 0xd0, 0xb1, 0x55, 0x9c, 0x83,
            0xef, 0x5b, 0x04, 0x44, 0x5c, 0xc4, 0x58, 0x1c, 0x8e, 0x86, 0xd8, 0x22, 0x4e, 0xdd,
            0xd0, 0x9f, 0x11, 0x57,
        ],
        {
            let mut k = prime;
            k[0] -= 1;
            k
        },
    ];
    !low_order.contains(&key)
}

pub(crate) fn valid_query_encoding(query: &str) -> bool {
    let bytes = query.as_bytes();
    bytes.iter().enumerate().all(|(index, byte)| {
        *byte != b'%'
            || bytes
                .get(index + 1..index + 3)
                .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
    }) && percent_encoding::percent_decode_str(query)
        .decode_utf8()
        .is_ok()
}
