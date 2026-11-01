use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Connection {
    pub server_id: String,
    pub role: Role,
    pub version: u8,
    pub connection_id: Option<String>,
}

impl Connection {
    // Keep aligned with lib/paseo_relay/connection.ex, including validation order.
    pub fn from_query(query: &HashMap<String, String>) -> Result<Self, &'static str> {
        let get = |key: &str| query.get(key).map(String::as_str);
        let role = match get("role") {
            Some("server") => Role::Server,
            Some("client") => Role::Client,
            _ => return Err("Missing or invalid role parameter"),
        };
        let server_id = match get("serverId") {
            Some(value) if value.len() > 256 => return Err("serverId is too long"),
            Some(value) if !value.is_empty() => value.to_owned(),
            _ => return Err("Missing serverId parameter"),
        };
        let version = match get("v").unwrap_or("").trim() {
            "" | "1" => 1,
            "2" => 2,
            _ => return Err("Invalid v parameter (expected 1 or 2)"),
        };
        let connection_id = if version == 1 {
            None
        } else {
            let id = get("connectionId").unwrap_or("").trim();
            if id.len() > 256 {
                return Err("connectionId is too long");
            }
            Some(if id.is_empty() && role == Role::Client {
                let mut random = [0; 8];
                getrandom::fill(&mut random).map_err(|_| "Connection ID entropy unavailable")?;
                format!("conn_{:016x}", u64::from_be_bytes(random))
            } else {
                id.to_owned()
            })
        };
        Ok(Self {
            server_id,
            role,
            version,
            connection_id,
        })
    }
}
