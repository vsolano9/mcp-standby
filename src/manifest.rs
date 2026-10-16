// Manifests: the cached initialize result, lists and launch command of one server.
//
// Memory: the lists (some servers list megabytes of tool schemas) are never kept in memory. The proxy keeps
// only the small `initialize` and `discover` parts and reads a list slice from the file when a client
// asks for it; the buffer is freed right after the reply.
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Map, Value};

use crate::log::data_root;
use crate::timefmt::{iso_now, millis_now};

pub const SCHEMA: u64 = 1;

pub fn default_manifest_path(name: &str) -> PathBuf {
    data_root().join("manifests").join(format!("{name}.json"))
}

#[derive(Deserialize)]
struct RawManifest<'a> {
    schema: Option<Value>,
    #[serde(borrow)]
    initialize: Option<&'a RawValue>,
    #[serde(borrow)]
    tools: Option<&'a RawValue>,
    #[serde(borrow)]
    prompts: Option<&'a RawValue>,
    #[serde(borrow)]
    resources: Option<&'a RawValue>,
    #[serde(borrow, rename = "resourceTemplates")]
    resource_templates: Option<&'a RawValue>,
    #[serde(borrow)]
    discover: Option<&'a RawValue>,
    #[serde(borrow)]
    launch: Option<&'a RawValue>,
    #[serde(borrow)]
    negotiated: Option<&'a RawValue>,
}

impl<'a> RawManifest<'a> {
    fn list(&self, kind: &str) -> Option<&'a RawValue> {
        match kind {
            "tools" => self.tools,
            "prompts" => self.prompts,
            "resources" => self.resources,
            "resourceTemplates" => self.resource_templates,
            _ => None,
        }
    }
}

fn is_array(r: &RawValue) -> bool {
    r.get().trim_start().starts_with('[')
}

/// The parts of a valid manifest the proxy keeps in memory.
#[derive(Clone)]
pub struct Head {
    pub initialize: Value,
    pub discover: Option<Value>,
    pub launch: Option<Value>,
    // The protocol version the server agreed on for each version a client asked for, as seen in sessions.
    pub negotiated: Map<String, Value>,
}

impl Head {
    pub fn capability(&self, cap: &str) -> bool {
        truthy(self.initialize.get("capabilities").and_then(|c| c.get(cap)))
    }
}

/// JavaScript truthiness for JSON values (undefined, null, false, 0, "" are falsy).
pub fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::Number(n)) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Some(Value::String(s)) => !s.is_empty(),
        _ => true,
    }
}

/// readManifest(): null unless schema 1, `initialize` with `serverInfo`, and `tools` an array.
pub fn read_head(file: &Path) -> Option<Head> {
    let bytes = fs::read(file).ok()?;
    let raw: RawManifest = serde_json::from_slice(&bytes).ok()?;
    if raw.schema.as_ref().and_then(|v| v.as_u64()) != Some(SCHEMA) {
        return None;
    }
    let init: Value = serde_json::from_str(raw.initialize?.get()).ok()?;
    if !init.is_object() || init.get("serverInfo").map(|v| !truthy(Some(v))).unwrap_or(true) {
        return None;
    }
    if !raw.tools.map(is_array).unwrap_or(false) {
        return None;
    }
    let parse = |r: Option<&RawValue>| r.and_then(|r| serde_json::from_str::<Value>(r.get()).ok()).filter(|v| !v.is_null());
    Some(Head {
        initialize: init,
        discover: parse(raw.discover),
        launch: parse(raw.launch),
        negotiated: match parse(raw.negotiated) {
            Some(Value::Object(m)) => m,
            _ => Map::new(),
        },
    })
}

/// One list, as raw JSON text (`[]` when the manifest has none), plus a hash of exactly those bytes.
pub fn read_list(file: &Path, kind: &str) -> Option<(String, u64)> {
    let bytes = fs::read(file).ok()?;
    let raw: RawManifest = serde_json::from_slice(&bytes).ok()?;
    if raw.schema.as_ref().and_then(|v| v.as_u64()) != Some(SCHEMA) || !raw.tools.map(is_array).unwrap_or(false) {
        return None;
    }
    let text = match raw.list(kind) {
        // Manifests are pretty-printed; the wire format is one message per line.
        Some(r) if is_array(r) => minify(r.get()),
        Some(r) if r.get().trim() == "null" => "[]".to_string(),
        None => "[]".to_string(),
        Some(_) => return None,
    };
    let h = fnv(text.as_bytes());
    Some((text, h))
}

/// Drop the insignificant whitespace of valid JSON text (everything outside strings).
fn minify(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let (mut in_str, mut esc) = (false, false);
    for ch in json.chars() {
        if in_str {
            out.push(ch);
            if esc {
                esc = false;
            } else if ch == '\\' {
                esc = true;
            } else if ch == '"' {
                in_str = false;
            }
            continue;
        }
        match ch {
            ' ' | '\t' | '\n' | '\r' => {}
            '"' => {
                in_str = true;
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

pub fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// The whole manifest as a JSON value (for the background drift check).
pub fn read_full(file: &Path) -> Option<Value> {
    let bytes = fs::read(file).ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    if v.get("schema").and_then(|s| s.as_u64()) != Some(SCHEMA) {
        return None;
    }
    Some(v)
}

/// Atomic write: temp file in the same directory, then rename over the target.
pub fn write_manifest(file: &Path, manifest: &Value) -> Result<(), String> {
    if let Some(dir) = file.parent() {
        fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let tmp = PathBuf::from(format!("{}.{}.{}.{}.tmp", file.display(), std::process::id(), millis_now(), seq));
    let mut text = serde_json::to_string_pretty(manifest).map_err(|e| e.to_string())?;
    text.push('\n');
    fs::write(&tmp, text).map_err(|e| e.to_string())?;
    let mut last = String::new();
    for i in 0..8u64 {
        match fs::rename(&tmp, file) {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = e.to_string();
                // Windows may briefly lock the target (a reader, an antivirus scan).
                std::thread::sleep(std::time::Duration::from_millis(25 * (i + 1)));
            }
        }
    }
    let _ = fs::remove_file(&tmp);
    Err(last)
}

pub struct Lists {
    pub tools: Vec<Value>,
    pub prompts: Vec<Value>,
    pub resources: Vec<Value>,
    pub resource_templates: Vec<Value>,
}

impl Lists {
    pub fn get(&self, kind: &str) -> &Vec<Value> {
        match kind {
            "tools" => &self.tools,
            "prompts" => &self.prompts,
            "resources" => &self.resources,
            _ => &self.resource_templates,
        }
    }
}

/// The `initialize` block a manifest stores: protocolVersion, capabilities ({} when absent),
/// serverInfo, instructions (only when present).
pub fn init_block(init_result: &Value) -> Value {
    let mut m = Map::new();
    m.insert(
        "protocolVersion".into(),
        init_result.get("protocolVersion").cloned().unwrap_or(Value::Null),
    );
    m.insert("capabilities".into(), capabilities_or_empty(init_result));
    m.insert("serverInfo".into(), init_result.get("serverInfo").cloned().unwrap_or(Value::Null));
    if let Some(i) = init_result.get("instructions") {
        if truthy(Some(i)) {
            m.insert("instructions".into(), i.clone());
        }
    }
    Value::Object(m)
}

pub fn capabilities_or_empty(init_result: &Value) -> Value {
    match init_result.get("capabilities") {
        Some(c) if truthy(Some(c)) => c.clone(),
        _ => json!({}),
    }
}

pub fn build_manifest(name: &str, command: &str, args: &[String], init_result: &Value, lists: Lists, discover: Option<Value>) -> Value {
    let mut m = Map::new();
    m.insert("schema".into(), json!(SCHEMA));
    m.insert("name".into(), json!(name));
    m.insert("fetchedAt".into(), json!(iso_now()));
    m.insert("launch".into(), json!({ "command": command, "args": args }));
    m.insert("initialize".into(), init_block(init_result));
    m.insert("tools".into(), Value::Array(lists.tools));
    m.insert("prompts".into(), Value::Array(lists.prompts));
    m.insert("resources".into(), Value::Array(lists.resources));
    m.insert("resourceTemplates".into(), Value::Array(lists.resource_templates));
    if let Some(d) = discover {
        m.insert("discover".into(), d);
    }
    Value::Object(m)
}

// ---- order-insensitive list comparison ----

fn key_field(kind: &str) -> &'static str {
    match kind {
        "tools" | "prompts" => "name",
        "resources" => "uri",
        _ => "uriTemplate",
    }
}

// JavaScript String(v) for the sort key.
fn js_string(v: Option<&Value>) -> String {
    match v {
        None => "undefined".into(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) => "null".into(),
        Some(other) => other.to_string(),
    }
}

// Serialize with object keys sorted at every level.
fn canon(v: &Value, out: &mut String) {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String((*k).clone()).to_string());
                out.push(':');
                canon(&m[*k], out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canon(x, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

fn norm(kind: &str, list: &[Value]) -> String {
    let k = key_field(kind);
    let mut items: Vec<&Value> = list.iter().collect();
    items.sort_by_key(|a| js_string(a.get(k)));
    let mut out = String::new();
    out.push('[');
    for (i, x) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        canon(x, &mut out);
    }
    out.push(']');
    out
}

pub fn lists_equal(kind: &str, a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len() && norm(kind, a) == norm(kind, b)
}
