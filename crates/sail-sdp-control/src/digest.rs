//! Canonical request encoding: versioned, domain-separated, map keys sorted,
//! vectors in their meaningful order, no floating-point JSON. The digest of
//! a request is what the operation ledger compares; the same request id
//! with a different digest is rejected.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// The encoding version; a change here changes every digest.
pub const ENCODING_VERSION: &str = "sdp-control-v1";

/// Serialise `value` canonically under `domain` and hash it.
pub fn digest<T: Serialize>(domain: &str, value: &T) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    let mut hasher = Sha256::new();
    hasher.update(ENCODING_VERSION.as_bytes());
    hasher.update(b"\n");
    hasher.update(domain.as_bytes());
    hasher.update(b"\n");
    hasher.update(canonical(&value).as_bytes());
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Canonical text: objects with sorted keys, arrays in order, strings
/// escaped by the JSON rules, integers as decimal; floats are refused by
/// rendering them as tagged strings so two encoders cannot disagree.
pub fn canonical(value: &Value) -> String {
    let mut out = String::new();
    write(value, &mut out);
    out
}

fn write(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push_str(&i.to_string());
            } else if let Some(u) = n.as_u64() {
                out.push_str(&u.to_string());
            } else {
                out.push_str(&format!("\"float:{n}\""));
            }
        }
        Value::String(s) => out.push_str(&serde_json::to_string(s).unwrap_or_default()),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap_or_default());
                out.push(':');
                write(&map[*key], out);
            }
            out.push('}');
        }
    }
}
