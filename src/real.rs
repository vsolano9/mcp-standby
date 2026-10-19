// A real MCP server child process speaking newline-delimited JSON-RPC on stdio.
//
// Threads per running server: a stdin writer (so a slow server never blocks the proxy), a stdout
// reader (answers the proxy's own `mcp-standby/` requests itself and hands everything else to the main
// loop) and a stderr pump into the log.
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Map, Value};

use crate::log::Logger;
use crate::manifest::Lists;
use crate::spawn::{kill_tree, start_process, Launch};

pub const PREFIX: &str = "mcp-standby/";

// A random tag in every id the proxy sends, so a client id cannot match a pending one by accident.
pub fn id_tag() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static TAG: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *TAG.get_or_init(|| {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u32(std::process::id());
        h.finish()
    })
}

/// The start of every id the proxy itself puts on the wire (its own requests, and the ids it gives
/// server-initiated requests towards the client).
pub fn own_prefix() -> String {
    format!("{}{:016x}/", PREFIX, id_tag())
}

pub enum ServerEvent {
    /// A message for the client: the raw JSON text and, for a response, the normalized id key.
    Line {
        gen: u64,
        text: String,
        response_key: Option<String>,
    },
    Exit {
        gen: u64,
        why: String,
    },
}

pub type Sink = Arc<dyn Fn(ServerEvent) + Send + Sync>;

#[derive(Debug)]
pub enum CallError {
    Rpc { method: String, error: Value },
    Other(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Rpc { method, error } => {
                write!(
                    f,
                    "{}: {}",
                    method,
                    error.get("message").and_then(|m| m.as_str()).unwrap_or("error")
                )
            }
            CallError::Other(s) => f.write_str(s),
        }
    }
}

type Waiter = (String, Sender<Result<Value, CallError>>);

pub struct RealServer {
    pub pid: u32,
    gen: u64,
    log: Arc<Logger>,
    writer: Mutex<Option<Sender<Vec<u8>>>>,
    pending: Mutex<HashMap<String, Waiter>>,
    seq: AtomicU64,
    dead: AtomicBool,
    // Set once the process has been reaped, so a later kill never targets a reused PID.
    exited: AtomicBool,
    // Set when the proxy stops reading this server: it was killed, or it exited and a process it left behind
    // holds its stdout open. Lines read after that are dropped.
    abandoned: AtomicBool,
    // Bytes read from the server's stdout, so the exit path can tell a pipe that is still draining.
    read_bytes: Arc<AtomicU64>,
    pub init_result: Mutex<Option<Value>>,
}

#[derive(Deserialize)]
struct Peek<'a> {
    #[serde(borrow)]
    id: Option<&'a RawValue>,
    #[serde(borrow)]
    method: Option<&'a RawValue>,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

pub fn id_key(raw: &RawValue) -> String {
    serde_json::from_str::<Value>(raw.get())
        .map(|v| v.to_string())
        .unwrap_or_else(|_| raw.get().to_string())
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

impl RealServer {
    pub fn spawn(launch: &Launch, log: Arc<Logger>, gen: u64, events: Sink) -> Result<Arc<RealServer>, String> {
        let mut child = start_process(launch)?;
        let pid = child.id();
        // Arguments are not logged: some launchers take tokens as arguments. Pass secrets through env.
        log.info(&format!(
            "spawned pid={} {} ({} argument(s))",
            pid,
            launch.command,
            launch.args.len()
        ));
        let abort = |e: String| -> String {
            kill_tree(pid);
            e
        };
        let stdin = child.stdin.take().ok_or_else(|| abort("no stdin pipe".into()))?;
        let stdout = child.stdout.take().ok_or_else(|| abort("no stdout pipe".into()))?;
        let stderr = child.stderr.take().ok_or_else(|| abort("no stderr pipe".into()))?;
        let (wtx, wrx) = channel::<Vec<u8>>();
        let server = Arc::new(RealServer {
            pid,
            gen,
            log: log.clone(),
            writer: Mutex::new(Some(wtx)),
            pending: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            exited: AtomicBool::new(false),
            abandoned: AtomicBool::new(false),
            read_bytes: Arc::new(AtomicU64::new(0)),
            init_result: Mutex::new(None),
        });
        std::thread::Builder::new()
            .name("server-stdin".into())
            .spawn(move || {
                let mut stdin = stdin;
                for line in wrx {
                    if stdin.write_all(&line).is_err() || stdin.flush().is_err() {
                        break;
                    }
                }
            })
            .map_err(|e| abort(e.to_string()))?;
        let log2 = log.clone();
        std::thread::Builder::new()
            .name("server-stderr".into())
            .spawn(move || {
                let mut stderr = stderr;
                let mut buf = [0u8; 8192];
                loop {
                    match stderr.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => log2.raw(&buf[..n]),
                    }
                }
            })
            .map_err(|e| abort(e.to_string()))?;
        let s2 = server.clone();
        let counted = server.read_bytes.clone();
        let line_events = events.clone();
        let (eof_tx, eof_rx) = channel::<()>();
        std::thread::Builder::new()
            .name("server-stdout".into())
            .spawn(move || {
                let mut reader = BufReader::new(Counted { inner: stdout, n: counted });
                let mut buf = Vec::new();
                loop {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    let mut end = buf.len();
                    while end > 0 && (buf[end - 1] == b'\n' || buf[end - 1] == b'\r') {
                        end -= 1;
                    }
                    let line = String::from_utf8_lossy(&buf[..end]);
                    if line.trim().is_empty() {
                        continue;
                    }
                    s2.on_line(&line, &line_events);
                }
                let _ = eof_tx.send(());
            })
            .map_err(|e| abort(e.to_string()))?;
        // The exit is taken from the process itself: a child that inherited the server's stdout can keep
        // the pipe open long after the server is gone.
        let s3 = server.clone();
        std::thread::Builder::new()
            .name("server-wait".into())
            .spawn(move || {
                let why = match child.wait() {
                    Ok(st) => exit_text(&st),
                    Err(e) => format!("exited (wait failed: {e})"),
                };
                s3.exited.store(true, Ordering::SeqCst);
                // Let the reader pass on everything the server wrote before it exited: wait for the end of the
                // pipe while bytes keep arriving. Half a second without any (or 10 s in all) means a process the
                // server left behind holds the pipe open; stop reading it.
                let mut seen = s3.read_bytes.load(Ordering::SeqCst);
                let mut eof = false;
                for _ in 0..20 {
                    if !matches!(eof_rx.recv_timeout(Duration::from_millis(500)), Err(RecvTimeoutError::Timeout)) {
                        eof = true;
                        break;
                    }
                    let now = s3.read_bytes.load(Ordering::SeqCst);
                    if now == seen {
                        break;
                    }
                    seen = now;
                }
                if !eof {
                    s3.abandoned.store(true, Ordering::SeqCst);
                }
                s3.died(&why);
                events(ServerEvent::Exit { gen: s3.gen, why });
            })
            .map_err(|e| abort(e.to_string()))?;
        Ok(server)
    }

    fn on_line(&self, line: &str, events: &Sink) {
        if self.abandoned.load(Ordering::SeqCst) {
            self.log
                .debug(&format!("line after server pid={} stopped, dropped: {}", self.pid, clip(line, 200)));
            return;
        }
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            match serde_json::from_str::<Vec<&RawValue>>(trimmed) {
                Ok(items) => {
                    for item in items {
                        self.on_message(item.get(), events);
                    }
                }
                Err(_) => self.log.warn(&format!("non-JSON line from server ignored: {}", clip(line, 200))),
            }
            return;
        }
        self.on_message(line, events);
    }

    fn on_message(&self, text: &str, events: &Sink) {
        let peek: Peek = match serde_json::from_str(text) {
            Ok(p) => p,
            Err(_) => {
                self.log.warn(&format!("non-JSON line from server ignored: {}", clip(text, 200)));
                return;
            }
        };
        // A ping from the server is answered here: MCP allows one before initialization completes, when the
        // client cannot see it yet, and the answer does not depend on the client.
        if let (Some(m), Some(id)) = (peek.method, peek.id) {
            if m.get() == "\"ping\"" {
                self.send_text(&format!("{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{}}}}", id.get()));
                return;
            }
        }
        let mut response_key = None;
        if peek.method.is_none() {
            if let Some(id) = peek.id {
                if let Ok(s) = serde_json::from_str::<String>(id.get()) {
                    let ours = s.starts_with(&own_prefix());
                    let waiter = if ours { lock(&self.pending).remove(&s) } else { None };
                    if ours && waiter.is_none() {
                        // An answer to one of the proxy's own requests that already timed out.
                        self.log.debug(&format!("late answer to {s} dropped"));
                        return;
                    }
                    if let Some((method, tx)) = waiter {
                        let r = match (peek.error, peek.result) {
                            (Some(e), _) if e.get().trim() != "null" => Err(CallError::Rpc {
                                method,
                                error: serde_json::from_str(e.get()).unwrap_or(Value::Null),
                            }),
                            (_, Some(r)) => Ok(serde_json::from_str(r.get()).unwrap_or(Value::Null)),
                            _ => Ok(Value::Null),
                        };
                        let _ = tx.send(r);
                        return;
                    }
                }
                response_key = Some(id_key(id));
            }
        }
        events(ServerEvent::Line {
            gen: self.gen,
            text: text.to_string(),
            response_key,
        });
    }

    fn died(&self, why: &str) {
        if self.dead.swap(true, Ordering::SeqCst) {
            return;
        }
        self.log.warn(&format!("server pid={} {}", self.pid, why));
        let waiters: Vec<Waiter> = lock(&self.pending).drain().map(|(_, w)| w).collect();
        for (method, tx) in waiters {
            let _ = tx.send(Err(CallError::Other(format!("server {why} while waiting for {method}"))));
        }
        lock(&self.writer).take();
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// True once the proxy stopped this server itself (shutdown or replacement), not when it died.
    pub fn is_abandoned(&self) -> bool {
        self.abandoned.load(Ordering::SeqCst)
    }

    pub fn send_text(&self, text: &str) -> bool {
        if self.is_dead() {
            return false;
        }
        let mut line = Vec::with_capacity(text.len() + 1);
        line.extend_from_slice(text.as_bytes());
        line.push(b'\n');
        match lock(&self.writer).as_ref() {
            Some(w) => w.send(line).is_ok(),
            None => false,
        }
    }

    pub fn notify(&self, method: &str) {
        self.send_text(&json!({ "jsonrpc": "2.0", "method": method }).to_string());
    }

    pub fn request(&self, method: &str, params: Option<Value>, timeout: Duration) -> Result<Value, CallError> {
        let id = format!("{}{}", own_prefix(), self.seq.fetch_add(1, Ordering::SeqCst) + 1);
        let (tx, rx) = channel();
        lock(&self.pending).insert(id.clone(), (method.to_string(), tx));
        let mut msg = Map::new();
        msg.insert("jsonrpc".into(), json!("2.0"));
        msg.insert("id".into(), json!(id));
        msg.insert("method".into(), json!(method));
        if let Some(p) = params {
            msg.insert("params".into(), p);
        }
        if !self.send_text(&Value::Object(msg).to_string()) {
            lock(&self.pending).remove(&id);
            return Err(CallError::Other("server not writable".into()));
        }
        match rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(RecvTimeoutError::Timeout) => {
                lock(&self.pending).remove(&id);
                Err(CallError::Other(format!(
                    "timeout waiting for {} after {} ms",
                    method,
                    timeout.as_millis()
                )))
            }
            Err(RecvTimeoutError::Disconnected) => Err(CallError::Other(format!("server gone while waiting for {method}"))),
        }
    }

    pub fn handshake(&self, init_params: Value, timeout: Duration) -> Result<Value, CallError> {
        let r = self.request("initialize", Some(init_params), timeout)?;
        *lock(&self.init_result) = Some(r.clone());
        self.notify("notifications/initialized");
        Ok(r)
    }

    /// Fetch every list the server's capabilities advertise, following cursors.
    pub fn fetch_lists(&self, timeout: Duration) -> Result<Lists, CallError> {
        let caps = lock(&self.init_result)
            .as_ref()
            .and_then(|i| i.get("capabilities").cloned())
            .unwrap_or(Value::Null);
        let has = |c: &str| crate::manifest::truthy(caps.get(c));
        let spec: [(&str, &str, bool); 4] = [
            ("tools/list", "tools", has("tools")),
            ("prompts/list", "prompts", has("prompts")),
            ("resources/list", "resources", has("resources")),
            ("resources/templates/list", "resourceTemplates", has("resources")),
        ];
        let mut out: Vec<Vec<Value>> = Vec::new();
        for (method, field, enabled) in spec {
            let mut items = Vec::new();
            if enabled {
                let mut cursor: Option<Value> = None;
                let mut seen: Vec<String> = Vec::new();
                loop {
                    let params = match &cursor {
                        Some(c) => json!({ "cursor": c }),
                        None => json!({}),
                    };
                    let res = match self.request(method, Some(params), timeout) {
                        Ok(r) => r,
                        // resources/templates/list is optional even when resources is advertised.
                        Err(CallError::Rpc { error, .. }) if error.get("code").and_then(|c| c.as_i64()) == Some(-32601) => break,
                        Err(e) => return Err(e),
                    };
                    if let Some(Value::Array(a)) = res.get(field) {
                        items.extend(a.iter().cloned());
                    }
                    // Cursors are opaque: any present value, the empty string included, asks for the next page.
                    cursor = res.get("nextCursor").filter(|c| !c.is_null()).cloned();
                    match &cursor {
                        Some(c) => {
                            let k = c.to_string();
                            if seen.contains(&k) {
                                break;
                            }
                            seen.push(k);
                        }
                        None => break,
                    }
                }
            }
            out.push(items);
        }
        let mut it = out.into_iter();
        Ok(Lists {
            tools: it.next().unwrap_or_default(),
            prompts: it.next().unwrap_or_default(),
            resources: it.next().unwrap_or_default(),
            resource_templates: it.next().unwrap_or_default(),
        })
    }

    /// Ask for an optional method and return {result} or {error} verbatim (never fails on RPC errors).
    pub fn probe(&self, method: &str) -> Option<Value> {
        match self.request(method, Some(json!({})), Duration::from_secs(15)) {
            Ok(r) => Some(json!({ "result": r })),
            Err(CallError::Rpc { error, .. }) => Some(json!({ "error": error })),
            Err(_) => None,
        }
    }

    pub fn kill(&self) {
        self.abandoned.store(true, Ordering::SeqCst);
        self.dead.store(true, Ordering::SeqCst);
        lock(&self.pending).clear();
        // Kill the tree before closing the server's stdin: a server that exits on EOF first would leave
        // its children orphaned, and an exited server's PID is never signalled (it may have been reused).
        if !self.exited.load(Ordering::SeqCst) {
            kill_tree(self.pid);
        }
        lock(&self.writer).take();
    }
}

struct Counted<R> {
    inner: R,
    n: Arc<AtomicU64>,
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let k = self.inner.read(buf)?;
        self.n.fetch_add(k as u64, Ordering::SeqCst);
        Ok(k)
    }
}

fn exit_text(st: &std::process::ExitStatus) -> String {
    let code = st.code().map(|c| c.to_string()).unwrap_or_else(|| "null".into());
    #[cfg(unix)]
    let signal = std::os::unix::process::ExitStatusExt::signal(st)
        .map(|s| s.to_string())
        .unwrap_or_else(|| "null".into());
    #[cfg(not(unix))]
    let signal = "null";
    format!("exited code={code} signal={signal}")
}

pub fn clip(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}
