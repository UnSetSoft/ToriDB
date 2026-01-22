use std::collections::HashMap;
use regex::Regex;

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct ConnectionUri {
    pub username: Option<String>,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub db_name: Option<String>,
    pub query: HashMap<String, String>,
}

impl ConnectionUri {
    pub fn parse(uri: &str) -> Result<Self, String> {
        // Format: db://[username:password+]host[:port][/dbName][?query]
        // Note: the + is used as a separator between auth and host to avoid ambiguity with :port
        let re = Regex::new(r"^db://(?:(?:([^:]+):([^@+]+)[@+])?)([^:/\?]+)(?::(\d+))?(?:/([^?]+))?(?:\?(.*))?$").map_err(|e| e.to_string())?;
        
        if let Some(caps) = re.captures(uri) {
            let username = caps.get(1).map(|m| m.as_str().to_string());
            let password = caps.get(2).map(|m| m.as_str().to_string());
            let host = caps.get(3).unwrap().as_str().to_string();
            let port = caps.get(4)
                .map(|m| m.as_str().parse::<u16>().map_err(|_| "Invalid port".to_string()))
                .transpose()?
                .unwrap_or(8569);
            let db_name = caps.get(5).map(|m| m.as_str().to_string());
            
            let mut query = HashMap::new();
            if let Some(query_str) = caps.get(6) {
                for pair in query_str.as_str().split('&') {
                    let mut parts = pair.splitn(2, '=');
                    if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                        query.insert(key.to_string(), value.to_string());
                    }
                }
            }

            Ok(ConnectionUri {
                username,
                password,
                host,
                port,
                db_name,
                query,
            })
        } else {
            Err("Invalid URI format. Expected: db://[user:pass+]host[:port][/...][?...]".to_string())
        }
    }

    #[allow(dead_code)]
    pub fn to_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn db_name_default(&self) -> String {
        self.db_name.clone().unwrap_or_else(|| "data".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_uri() {
        let uri = ConnectionUri::parse("db://admin:secret+localhost:8569/production?timeout=5000&reconnect=true").unwrap();
        assert_eq!(uri.username, Some("admin".to_string()));
        assert_eq!(uri.password, Some("secret".to_string()));
        assert_eq!(uri.host, "localhost");
        assert_eq!(uri.port, 8569);
        assert_eq!(uri.db_name, Some("production".to_string()));
        assert_eq!(uri.query.get("timeout").unwrap(), "5000");
    }

    #[test]
    fn test_minimal_uri() {
        let uri = ConnectionUri::parse("db://127.0.0.1").unwrap();
        assert_eq!(uri.host, "127.0.0.1");
        assert_eq!(uri.port, 8569);
        assert_eq!(uri.username, None);
    }
}
