use base64::{Engine, engine::general_purpose::STANDARD};

pub const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024 - 14;
pub const MAX_CONTROL_BYTES: usize = 64 * 1024;

// Keep aligned with lib/paseo_relay/handshake_validation.ex; payloads otherwise stay opaque.
pub fn valid_handshake(payload: &[u8]) -> bool {
    let Ok(message) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return true;
    };
    if !matches!(
        message.get("type").and_then(|v| v.as_str()),
        Some("hello" | "e2ee_hello")
    ) {
        return true;
    }
    let Some(encoded) = message.get("key").and_then(|v| v.as_str()) else {
        return false;
    };
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
