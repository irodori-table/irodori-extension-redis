use irodori_connector_abi::{collect_url_auth, option_bool, option_string, push_sensitive};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Map, Value};

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, RedisConnection>>> = OnceLock::new();

struct RedisConnection {
    conn: redis::Connection,
    config: RedisConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisConfig {
    url: String,
    database: i64,
    redaction_values: Vec<String>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, RedisConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match RedisConfig::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = match redis::Client::open(config.url.as_str()) {
        Ok(client) => client,
        Err(err) => return abi::error("connector.invalidRequest", config.redact(&err.to_string())),
    };
    let mut conn = match client.get_connection() {
        Ok(conn) => conn,
        Err(err) => return abi::error("connector.connectFailed", config.redact(&err.to_string())),
    };
    let version = redis_version(&mut conn).unwrap_or_else(|| "Redis".to_string());

    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ("database".to_string(), json!(config.database)),
        ("serverVersion".to_string(), Value::String(version)),
    ]);
    guard.insert(connection_id, RedisConnection { conn, config });
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(command) = abi::string_field(request, "command")
        .or_else(|| abi::string_field(request, "sql"))
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string command, sql, query, or statement field.",
        );
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let Some(connection) = guard.get_mut(&connection_id) else {
        return abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        );
    };
    match run_command(&mut connection.conn, command, abi::max_rows(request)) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let Some(connection) = guard.get_mut(&connection_id) else {
        return abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        );
    };
    match load_metadata(&mut connection.conn, connection.config.database) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl RedisConfig {
    fn from_request(request: &Value) -> Result<Self, String> {
        let database = option_string(request, &["database", "db"])
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let url = option_string(request, &["connectionString", "url", "dsn"])
            .unwrap_or_else(|| build_url(request, database));
        let mut redaction_values = Vec::new();
        push_sensitive(
            &mut redaction_values,
            option_string(request, &["password"]).as_deref(),
        );
        push_sensitive(
            &mut redaction_values,
            option_string(request, &["token"]).as_deref(),
        );
        collect_url_auth(&url, &mut redaction_values);
        Ok(Self {
            url,
            database,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values.iter().fold(
            message.replace(&self.url, "<redis-url>"),
            |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            },
        )
    }
}

fn build_url(request: &Value, database: i64) -> String {
    let host = option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
    let port = option_string(request, &["port"]).unwrap_or_else(|| "6379".into());
    let username = option_string(request, &["user", "username"]);
    let password = option_string(request, &["password", "token"]);
    let scheme = if option_bool(request, &["tls", "ssl"]).unwrap_or(false) {
        "rediss"
    } else {
        "redis"
    };
    let auth = match (username, password) {
        (Some(user), Some(password)) => format!("{user}:{password}@"),
        (None, Some(password)) => format!(":{password}@"),
        _ => String::new(),
    };
    format!("{scheme}://{auth}{host}:{port}/{database}")
}

fn redis_version(conn: &mut redis::Connection) -> Option<String> {
    let info: String = redis::cmd("INFO").arg("server").query(conn).ok()?;
    for line in info.lines() {
        if let Some(version) = line.strip_prefix("redis_version:") {
            return Some(format!("Redis {}", version.trim()));
        }
    }
    Some("Redis".to_string())
}

fn run_command(
    conn: &mut redis::Connection,
    command: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let parts = split_args(command);
    if parts.is_empty() {
        return Err("No Redis command specified.".to_string());
    }
    let mut cmd = redis::cmd(&parts[0]);
    for arg in &parts[1..] {
        cmd.arg(arg);
    }
    let value: redis::Value = cmd.query(conn).map_err(|err| err.to_string())?;
    Ok(redis_value_to_output(value, cap))
}

fn load_metadata(conn: &mut redis::Connection, database: i64) -> Result<Value, String> {
    let mut cursor = 0_u64;
    let mut keys = Vec::new();
    loop {
        let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg("200")
            .query(conn)
            .map_err(|err| err.to_string())?;
        for key in batch {
            keys.push(key);
            if keys.len() >= 200 {
                break;
            }
        }
        cursor = next_cursor;
        if cursor == 0 || keys.len() >= 200 {
            break;
        }
    }
    let mut objects = Vec::new();
    for key in keys {
        let key_type: String = redis::cmd("TYPE")
            .arg(&key)
            .query(conn)
            .unwrap_or_else(|_| "unknown".to_string());
        objects.push(json!({
            "schema": format!("db{database}"),
            "name": key,
            "kind": "key",
            "columns": [{
                "name": "value",
                "dataType": key_type,
                "nullable": true,
                "ordinal": 1
            }],
            "indexes": [],
            "primaryKey": [],
            "foreignKeys": []
        }));
    }
    Ok(json!({
        "schemas": [{
            "name": format!("db{database}"),
            "objects": objects
        }]
    }))
}

fn redis_value_to_output(value: redis::Value, cap: usize) -> QueryOutput {
    match value {
        redis::Value::Nil => (vec!["value".to_string()], vec![vec![Value::Null]], false),
        redis::Value::Int(value) => (
            vec!["value".to_string()],
            vec![vec![Value::Number(value.into())]],
            false,
        ),
        redis::Value::BulkString(bytes) => (
            vec!["value".to_string()],
            vec![vec![Value::String(
                String::from_utf8_lossy(&bytes).into_owned(),
            )]],
            false,
        ),
        redis::Value::SimpleString(value) => (
            vec!["status".to_string()],
            vec![vec![Value::String(value)]],
            false,
        ),
        redis::Value::Okay => (
            vec!["status".to_string()],
            vec![vec![Value::String("OK".to_string())]],
            false,
        ),
        redis::Value::Array(values) => {
            let mut rows = Vec::new();
            let mut truncated = false;
            for (index, value) in values.into_iter().enumerate() {
                if rows.len() >= cap {
                    truncated = true;
                    break;
                }
                rows.push(vec![json!(index), redis_value_to_json(value)]);
            }
            (
                vec!["index".to_string(), "value".to_string()],
                rows,
                truncated,
            )
        }
        other => (
            vec!["value".to_string()],
            vec![vec![redis_value_to_json(other)]],
            false,
        ),
    }
}

fn redis_value_to_json(value: redis::Value) -> Value {
    match value {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(value) => Value::Number(value.into()),
        redis::Value::BulkString(bytes) => {
            Value::String(String::from_utf8_lossy(&bytes).into_owned())
        }
        redis::Value::SimpleString(value) => Value::String(value),
        redis::Value::Okay => Value::String("OK".to_string()),
        redis::Value::Array(values) => {
            Value::Array(values.into_iter().map(redis_value_to_json).collect())
        }
        redis::Value::Attribute { data, attributes } => json!({
            "data": redis_value_to_json(*data),
            "attributes": attributes
                .into_iter()
                .map(|(key, value)| json!([redis_value_to_json(key), redis_value_to_json(value)]))
                .collect::<Vec<_>>()
        }),
        redis::Value::Boolean(value) => Value::Bool(value),
        redis::Value::Double(value) => json!(value),
        redis::Value::Map(values) => Value::Array(
            values
                .into_iter()
                .map(|(key, value)| json!([redis_value_to_json(key), redis_value_to_json(value)]))
                .collect(),
        ),
        redis::Value::Set(values) => {
            Value::Array(values.into_iter().map(redis_value_to_json).collect())
        }
        redis::Value::VerbatimString { format, text } => json!({
            "format": format!("{format:?}"),
            "text": text
        }),
        redis::Value::BigNumber(value) => Value::String(value.to_string()),
        redis::Value::Push { kind, data } => json!({
            "kind": format!("{kind:?}"),
            "data": data.into_iter().map(redis_value_to_json).collect::<Vec<_>>()
        }),
        redis::Value::ServerError(err) => json!({
            "code": err.code(),
            "detail": err.details()
        }),
    }
}

fn split_args(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escape = false;
    for ch in input.chars() {
        if escape {
            current.push(ch);
            escape = false;
            continue;
        }
        if ch == '\\' {
            escape = true;
            continue;
        }
        if let Some(active_quote) = quote {
            if ch == active_quote {
                quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_args_handles_quotes_and_escapes() {
        assert_eq!(
            split_args(r#"SET "space key" hello\ world"#),
            vec!["SET", "space key", "hello world"]
        );
    }

    #[test]
    fn maps_array_values_to_rows() {
        let (columns, rows, truncated) = redis_value_to_output(
            redis::Value::Array(vec![
                redis::Value::BulkString(b"a".to_vec()),
                redis::Value::Int(2),
            ]),
            10,
        );
        assert_eq!(columns, vec!["index", "value"]);
        assert_eq!(rows[0], vec![json!(0), json!("a")]);
        assert_eq!(rows[1], vec![json!(1), json!(2)]);
        assert!(!truncated);
    }

    #[test]
    fn builds_url_from_profile_fields() {
        let request = json!({
            "profile": {
                "host": "localhost",
                "port": 6380,
                "database": "2",
                "password": "secret"
            }
        });
        let config = RedisConfig::from_request(&request).unwrap();
        assert_eq!(config.url, "redis://:secret@localhost:6380/2");
        assert_eq!(config.database, 2);
    }
}
