// The proxy: speaks MCP to the client from a cached manifest and starts the
// real server on the first request that actually needs it. All state lives on the main thread; the
// stdin reader, the server threads, the starter and the background refresh report through `Event`.
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::value::RawValue;
use serde_json::{json, Map, Value};

use crate::log::Logger;
use crate::manifest::{self, Head, Lists};
use crate::real::{RealServer, ServerEvent, Sink};
use crate::spawn::Launch;

pub const SUPPORTED_VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// method, capability, list kind (also the result field)
const LIST_METHODS: [(&str, &str, &str); 4] = [
    ("tools/list", "tools", "tools"),
    ("prompts/list", "prompts", "prompts"),
    ("resources/list", "resources", "resources"),
    ("resources/templates/list", "resources", "resourceTemplates"),
];
const KINDS: [&str; 4] = ["tools", "prompts", "resources", "resourceTemplates"];

fn changed_method(kind: &str) -> &'static str {
    match kind {
        "tools" => "notifications/tools/list_changed",
        "prompts" => "notifications/prompts/list_changed",
        _ => "notifications/resources/list_changed",
    }
}

pub struct StartOk {
    server: Arc<RealServer>,
    init_result: Value,
    head: Option<Head>,
}

pub enum Event {
    Client(Vec<u8>),
    ClientEof,
    Server(ServerEvent),
    Started {
        gen: u64,
        result: Result<StartOk, (Option<Arc<RealServer>>, String)>,
    },
    Refreshed {
        gen: u64,
        notify: Vec<&'static str>,
        init: Option<Value>,
        changed: bool,
    },
}

/// What a shutdown (stdin closed, stdout gone, panic) needs, shared with the starter thread.
pub struct Shared {
    pub shutting_down: bool,
    pub server: Option<Arc<RealServer>>,
}

#[derive(PartialEq, Clone, Copy)]
enum State {
    Idle,
    Starting,
    Ready,
}

struct Msg {
    v: Value,
    raw: String,
}

impl Msg {
    fn method(&self) -> Option<&str> {
        self.v.get("method").and_then(|m| m.as_str())
    }
    fn id(&self) -> Option<&Value> {
        self.v.get("id").filter(|v| !v.is_null())
    }
}

fn key_of(v: &Value) -> String {
    v.to_string()
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Write one JSON-RPC message (one line) to the client. Exits through `stop` when stdout is gone.
pub fn emit(text: &str) {
    let out = std::io::stdout();
    let mut o = out.lock();
    let ok = o
        .write_all(text.as_bytes())
        .and_then(|_| o.write_all(b"\n"))
        .and_then(|_| o.flush());
    drop(o);
    if ok.is_err() {
        crate::stop("stdout closed");
    }
}

pub struct Proxy {
    name: String,
    manifest_path: PathBuf,
    launch: Launch,
    log: Arc<Logger>,
    startup_timeout: Duration,
    head: Option<Head>,
    state: State,
    server: Option<Arc<RealServer>>,
    gen: u64,
    client_init_params: Option<Value>,
    client_initialized: bool,
    log_level: Arc<Mutex<Option<Value>>>,
    queue: Vec<Msg>,
    inflight: HashMap<String, Value>,
    served: HashMap<&'static str, u64>,
    eager_reply: Option<Value>,
    // One background list refresh at a time: a server that sends a burst of list_changed (one per tool
    // it registers) must not start overlapping refreshes, where a slow older one could write its stale
    // snapshot last. A request that arrives meanwhile runs once after the current one.
    refreshing: bool,
    refresh_again: Option<bool>,
    // Whether the running refresh was triggered by the server's list_changed, and how many such refreshes
    // in a row found nothing new. Each one waits twice as long as the one before (1 s up to 60 s), so a
    // server that answers every list with list_changed costs a list a minute, and no change is dropped.
    refresh_on_notice: bool,
    quiet_notices: u32,
    // Requests and notifications the server sends before the client has its initialize answer and has
    // sent notifications/initialized; delivered in order once both are true.
    backlog: Vec<String>,
    events: Sender<Event>,
    shared: Arc<Mutex<Shared>>,
}

impl Proxy {
    pub fn new(
        name: String,
        manifest_path: PathBuf,
        launch: Launch,
        log: Arc<Logger>,
        startup_timeout: Duration,
        events: Sender<Event>,
        shared: Arc<Mutex<Shared>>,
    ) -> Proxy {
        let head = manifest::read_head(&manifest_path);
        if head.is_none() {
            log.warn(&format!(
                "no usable manifest at {}; will start the real server eagerly",
                manifest_path.display()
            ));
        }
        Proxy {
            name,
            manifest_path,
            launch,
            log,
            startup_timeout,
            head,
            state: State::Idle,
            server: None,
            gen: 0,
            client_init_params: None,
            client_initialized: false,
            log_level: Arc::new(Mutex::new(None)),
            queue: Vec::new(),
            inflight: HashMap::new(),
            served: HashMap::new(),
            eager_reply: None,
            refreshing: false,
            refresh_again: None,
            refresh_on_notice: false,
            quiet_notices: 0,
            backlog: Vec::new(),
            events,
            shared,
        }
    }

    pub fn on_event(&mut self, ev: Event) {
        match ev {
            Event::Client(line) => self.on_client_line(&line),
            Event::ClientEof => {}
            Event::Server(ServerEvent::Line { gen, text, response_key }) => self.on_server_line(gen, &text, response_key),
            Event::Server(ServerEvent::Exit { gen, why }) => self.on_server_exit(gen, &why),
            Event::Started { gen, result } => self.on_started(gen, result),
            Event::Refreshed {
                gen,
                notify,
                init,
                changed,
            } => self.on_refreshed(gen, notify, init, changed),
        }
    }

    // ---- client -> proxy ----
    fn on_client_line(&mut self, line: &[u8]) {
        let text = String::from_utf8_lossy(line);
        let t = text.trim();
        if t.is_empty() {
            return;
        }
        if t.starts_with('[') {
            match serde_json::from_str::<Vec<&RawValue>>(t) {
                Ok(items) => {
                    for it in items {
                        if let Ok(v) = serde_json::from_str::<Value>(it.get()) {
                            self.on_client_message(Msg {
                                v,
                                raw: it.get().to_string(),
                            });
                        }
                    }
                }
                Err(_) => emit(&json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": "Parse error" } }).to_string()),
            }
            return;
        }
        match serde_json::from_str::<Value>(t) {
            Ok(v) => self.on_client_message(Msg { v, raw: t.to_string() }),
            Err(_) => emit(&json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": "Parse error" } }).to_string()),
        }
    }

    fn on_client_message(&mut self, msg: Msg) {
        if !msg.v.is_object() {
            return;
        }
        let has_method = msg.method().is_some();
        let is_req = has_method && msg.id().is_some();
        if is_req {
            return self.request(msg);
        }
        if has_method {
            return self.notification(msg);
        }
        // A response from the client to a server-initiated request.
        match (&self.state, &self.server) {
            (State::Ready, Some(s)) => {
                s.send_text(&msg.raw);
            }
            _ => self.log.debug("dropping client response; no server running"),
        }
    }

    fn reply(&self, id: &Value, result: Value) {
        let mut m = Map::new();
        m.insert("jsonrpc".into(), json!("2.0"));
        m.insert("id".into(), id.clone());
        m.insert("result".into(), result);
        emit(&Value::Object(m).to_string());
    }

    fn fail(&self, id: &Value, code: Option<Value>, message: Option<Value>) {
        let mut e = Map::new();
        if let Some(c) = code {
            e.insert("code".into(), c);
        }
        if let Some(m) = message {
            e.insert("message".into(), m);
        }
        let mut m = Map::new();
        m.insert("jsonrpc".into(), json!("2.0"));
        m.insert("id".into(), id.clone());
        m.insert("error".into(), Value::Object(e));
        emit(&Value::Object(m).to_string());
    }

    fn fail_msg(&self, id: &Value, code: i64, message: &str) {
        self.fail(id, Some(json!(code)), Some(json!(message)));
    }

    fn request(&mut self, msg: Msg) {
        let method = msg.method().unwrap_or_default().to_string();
        let id = msg.id().cloned().unwrap_or(Value::Null);
        let params = msg.v.get("params").cloned();
        if method == "initialize" {
            self.client_init_params = Some(params.clone().filter(|p| !p.is_null()).unwrap_or_else(|| json!({})));
            if self.head.is_some() {
                let r = self.init_result(params.as_ref());
                return self.reply(&id, r);
            }
            // Eager fallback: no manifest, so learn everything from the real server first.
            self.start(Some(id));
            return;
        }
        if method == "ping" {
            return self.reply(&id, json!({}));
        }
        if method == "server/discover" {
            if let Some(h) = &self.head {
                // Clients probe this before initialize; answer exactly as the real server did at refresh time.
                let d = h.discover.as_ref();
                if let Some(r) = d.and_then(|d| d.get("result")).filter(|r| manifest::truthy(Some(r))) {
                    return self.reply(&id, r.clone());
                }
                let e = d.and_then(|d| d.get("error")).filter(|e| manifest::truthy(Some(e)));
                return match e {
                    Some(e) => self.fail(&id, e.get("code").cloned(), e.get("message").cloned()),
                    None => self.fail_msg(&id, -32601, "Method not found"),
                };
            }
            // No manifest yet: initialize starts the server eagerly and records the real answer. Starting
            // one for the probe would start a second server when initialize arrives.
            return self.fail_msg(&id, -32601, "Method not found");
        }
        // The manifest answers lists only until the real server runs; after that the server's own answer
        // is the current one (it may have changed its lists and said so with list_changed).
        let cached = self.state != State::Ready;
        if let Some(&(_, cap, kind)) = LIST_METHODS.iter().find(|(m, _, _)| cached && *m == method) {
            if let Some(has_cap) = self.head.as_ref().map(|h| h.capability(cap)) {
                if !has_cap {
                    return self.fail_msg(&id, -32601, &format!("Method not found: {method}"));
                }
                match manifest::read_list(&self.manifest_path, kind) {
                    Some((text, hash)) => {
                        self.served.insert(kind, hash);
                        emit(&format!(
                            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"{}\":{}}}}}",
                            id, kind, text
                        ));
                        return;
                    }
                    None => self.log.warn(&format!(
                        "manifest unreadable at {}; forwarding {method}",
                        self.manifest_path.display()
                    )),
                }
                return self.forward(msg);
            }
        }
        let level = params.as_ref().and_then(|p| p.get("level")).cloned();
        if method == "logging/setLevel"
            && self.state != State::Ready
            && self.head.as_ref().map(|h| h.capability("logging")).unwrap_or(false)
        {
            *lock(&self.log_level) = level;
            return self.reply(&id, json!({}));
        }
        if method == "logging/setLevel" {
            *lock(&self.log_level) = level;
        }
        self.forward(msg);
    }

    fn notification(&mut self, msg: Msg) {
        let method = msg.method().unwrap_or_default().to_string();
        if method == "notifications/initialized" {
            self.client_initialized = true;
            self.flush_backlog();
            return;
        }
        if method == "notifications/cancelled" {
            if let Some(rid) = msg.v.get("params").and_then(|p| p.get("requestId")) {
                let k = key_of(rid);
                if let Some(i) = self.queue.iter().position(|q| q.id().map(|x| key_of(x) == k).unwrap_or(false)) {
                    self.queue.remove(i); // never forwarded; nothing to cancel
                    return;
                }
            }
        }
        if self.state == State::Ready {
            if let Some(s) = &self.server {
                s.send_text(&msg.raw);
                return;
            }
        }
        if self.state == State::Starting {
            self.queue.push(msg); // delivered in order after the server's handshake
            return;
        }
        self.log.debug(&format!("dropping {method}; no server running"));
    }

    fn forward(&mut self, msg: Msg) {
        if self.state == State::Ready {
            if let Some(s) = self.server.clone() {
                if let Some(id) = msg.id() {
                    self.inflight.insert(key_of(id), id.clone());
                }
                if !s.send_text(&msg.raw) {
                    self.fail_inflight("server not writable");
                }
                return;
            }
        }
        let what = format!(
            "{} (id {})",
            msg.method().unwrap_or_default(),
            msg.id().map(key_of).unwrap_or_default()
        );
        self.queue.push(msg);
        if self.state == State::Idle {
            self.log.info(&format!("starting real server for {what}"));
            self.start(None);
        }
    }

    fn init_result(&self, client_params: Option<&Value>) -> Value {
        let h = self.head.as_ref().expect("manifest head");
        let wanted = client_params.and_then(|p| p.get("protocolVersion")).and_then(|v| v.as_str());
        // The real server gets the client's own initialize parameters later, so answer what it negotiated
        // before: the client's version when the server already answered one at least as new, else the
        // recorded one (a server answers with its newest version when it lacks the one asked for).
        let known = |v: &&str| SUPPORTED_VERSIONS.contains(v);
        let wanted = wanted.filter(known);
        let recorded = h.initialize.get("protocolVersion").and_then(|v| v.as_str()).filter(known);
        let protocol = match (wanted, recorded) {
            (Some(w), Some(r)) if w > r => r,
            (Some(w), _) => w,
            (None, Some(r)) => r,
            (None, None) => SUPPORTED_VERSIONS[0],
        }
        .to_string();
        let mut caps = manifest::capabilities_or_empty(&h.initialize);
        // The proxy can announce list changes after a refresh, so advertise it.
        for c in ["tools", "prompts", "resources"] {
            if let Some(Value::Object(o)) = caps.get_mut(c) {
                o.insert("listChanged".into(), json!(true));
            }
        }
        let mut m = Map::new();
        m.insert("protocolVersion".into(), json!(protocol));
        m.insert("capabilities".into(), caps);
        m.insert("serverInfo".into(), h.initialize.get("serverInfo").cloned().unwrap_or(Value::Null));
        if let Some(i) = h.initialize.get("instructions").filter(|i| manifest::truthy(Some(i))) {
            m.insert("instructions".into(), i.clone());
        }
        Value::Object(m)
    }

    // ---- starting the real server ----
    fn start(&mut self, eager_id: Option<Value>) {
        // A server started before initialize is replaced (eager start); stop its tree so it does not linger.
        if let Some(old) = self.server.take() {
            old.kill();
            self.forget_shared(&old);
            self.fail_inflight("server restarted for initialize");
        }
        self.state = State::Starting;
        self.gen += 1;
        let gen = self.gen;
        let eager = eager_id.is_some();
        if eager {
            if let Some(old) = self.eager_reply.take() {
                self.fail_msg(&old, -32600, "mcp-standby: initialize sent again before the first one was answered");
            }
        }
        self.eager_reply = eager_id;
        self.backlog.clear();
        let launch = self.launch.clone();
        let log = self.log.clone();
        let tx = self.events.clone();
        let shared = self.shared.clone();
        let log_level = self.log_level.clone();
        let timeout = self.startup_timeout;
        let init_params = self.client_init_params.clone().unwrap_or_else(|| {
            json!({ "protocolVersion": SUPPORTED_VERSIONS[0], "capabilities": {}, "clientInfo": { "name": "mcp-standby", "version": VERSION } })
        });
        let spawned = std::thread::Builder::new().name("starter".into()).spawn(move || {
            let sink_tx = tx.clone();
            let sink: Sink = Arc::new(move |ev| {
                let _ = sink_tx.send(Event::Server(ev));
            });
            let server = {
                let mut sh = lock(&shared);
                if sh.shutting_down {
                    return;
                }
                match RealServer::spawn(&launch, log.clone(), gen, sink) {
                    Ok(s) => {
                        sh.server = Some(s.clone());
                        s
                    }
                    Err(e) => {
                        let _ = tx.send(Event::Started {
                            gen,
                            result: Err((None, e)),
                        });
                        return;
                    }
                }
            };
            let run = || -> Result<(Value, Option<Head>), String> {
                let init = server.handshake(init_params, timeout).map_err(|e| e.to_string())?;
                // The client gets its initialize answer now; the lists are collected in the background once the
                // server's own requests (roots, sampling) can reach the client, and written as the manifest.
                let head = eager.then(|| Head {
                    initialize: manifest::init_block(&init),
                    discover: None,
                    launch: Some(json!({ "command": launch.command, "args": launch.args })),
                });
                let level = lock(&log_level).clone();
                if let Some(level) = level {
                    let _ = server.request("logging/setLevel", Some(json!({ "level": level })), Duration::from_secs(15));
                }
                Ok((init, head))
            };
            let result = match run() {
                Ok((init_result, head)) => Ok(StartOk { server, init_result, head }),
                Err(e) => Err((Some(server), e)),
            };
            let _ = tx.send(Event::Started { gen, result });
        });
        if let Err(e) = spawned {
            let _ = self.events.send(Event::Started {
                gen,
                result: Err((None, format!("cannot start thread: {e}"))),
            });
        }
    }

    fn forget_shared(&self, server: &Arc<RealServer>) {
        let mut sh = lock(&self.shared);
        if sh.server.as_ref().map(|s| Arc::ptr_eq(s, server)).unwrap_or(false) {
            sh.server = None;
        }
    }

    fn fail_queue(&mut self, why: &str) {
        let queued: Vec<Msg> = std::mem::take(&mut self.queue);
        if let Some(id) = self.eager_reply.take() {
            self.fail_msg(&id, -32603, &format!("mcp-standby: failed to start server: {why}"));
        }
        for q in queued {
            if let Some(id) = q.id() {
                self.fail_msg(id, -32603, &format!("mcp-standby: failed to start server: {why}"));
            }
        }
    }

    fn on_started(&mut self, gen: u64, result: Result<StartOk, (Option<Arc<RealServer>>, String)>) {
        let shutting_down = lock(&self.shared).shutting_down;
        match result {
            Ok(ok) => {
                if gen != self.gen || shutting_down {
                    ok.server.kill();
                    self.forget_shared(&ok.server);
                    return;
                }
                if ok.server.is_dead() {
                    self.log.error("failed to start real server: it exited during startup");
                    self.forget_shared(&ok.server);
                    self.server = None;
                    self.state = State::Idle;
                    self.fail_queue("server exited during startup");
                    return;
                }
                self.server = Some(ok.server.clone());
                let eager = self.eager_reply.is_some();
                if let Some(id) = self.eager_reply.take() {
                    if let Some(h) = ok.head {
                        self.head = Some(h);
                    }
                    self.reply(&id, ok.init_result.clone());
                }
                self.state = State::Ready;
                self.log.info(&format!(
                    "real server ready (pid={}), flushing {} queued message(s)",
                    ok.server.pid,
                    self.queue.len()
                ));
                let queued: Vec<Msg> = std::mem::take(&mut self.queue);
                for m in queued {
                    if m.id().is_some() {
                        self.forward(m);
                    } else if let Some(s) = &self.server {
                        s.send_text(&m.raw);
                    }
                }
                self.flush_backlog();
                // After a deferred start the client was served the manifest: tell it what differs. After an
                // eager start it lists the live server itself; the manifest is only written.
                self.refresh_in_background(ok.server, gen, !eager, Duration::ZERO);
            }
            Err((server, e)) => {
                if let Some(s) = server {
                    s.kill();
                    self.forget_shared(&s);
                }
                if gen != self.gen {
                    return; // a replaced start; the current one answers for itself
                }
                self.log.error(&format!("failed to start real server: {e}"));
                self.fail_queue(&e);
                self.server = None;
                self.state = State::Idle;
            }
        }
    }

    fn on_server_line(&mut self, gen: u64, text: &str, response_key: Option<String>) {
        if gen != self.gen {
            return;
        }
        if let Some(k) = response_key {
            self.inflight.remove(&k);
            emit(text);
            return;
        }
        if self.state != State::Ready || !self.client_initialized {
            self.backlog.push(text.to_string());
            return;
        }
        self.deliver(gen, text);
    }

    fn flush_backlog(&mut self) {
        if self.state != State::Ready || !self.client_initialized {
            return;
        }
        let gen = self.gen;
        for text in std::mem::take(&mut self.backlog) {
            self.deliver(gen, &text);
        }
    }

    /// A request or notification from the server, once the client may receive it.
    fn deliver(&mut self, gen: u64, text: &str) {
        emit(text);
        // The server changed a list: the client hears it from the server itself (just forwarded); bring
        // the manifest up to date so the next session's discovery is not stale either.
        if text.contains("list_changed") {
            let method = serde_json::from_str::<Value>(text)
                .ok()
                .and_then(|v| v.get("method").and_then(|m| m.as_str()).map(|m| m.to_string()));
            if method
                .map(|m| m.starts_with("notifications/") && m.ends_with("/list_changed"))
                .unwrap_or(false)
            {
                if let Some(server) = self.server.clone() {
                    let wait = self.notice_wait();
                    self.refresh_in_background(server, gen, false, wait);
                }
            }
        }
    }

    fn fail_inflight(&mut self, why: &str) {
        let ids: Vec<Value> = self.inflight.drain().map(|(_, id)| id).collect();
        for id in ids {
            self.fail_msg(&id, -32603, &format!("mcp-standby: {why}"));
        }
    }

    fn on_server_exit(&mut self, gen: u64, why: &str) {
        if gen != self.gen || self.state == State::Starting {
            return; // a stale server, or the starter reports the failure itself
        }
        if let Some(s) = self.server.take() {
            self.forget_shared(&s);
        }
        self.state = State::Idle;
        self.backlog.clear();
        if lock(&self.shared).shutting_down {
            return;
        }
        self.fail_inflight(&format!("server process {why}; it will be restarted on the next request"));
        self.log.warn("real server is gone; the next request restarts it");
    }

    // After a deferred start (or a list_changed from the server), compare the live lists with the manifest
    // and rewrite it when they differ. `tell_client`: also send list_changed for what differs from what
    // the client was served from the manifest.
    fn notice_wait(&self) -> Duration {
        Duration::from_secs((1u64 << self.quiet_notices.min(6)).min(60))
    }

    fn refresh_in_background(&mut self, server: Arc<RealServer>, gen: u64, tell_client: bool, wait: Duration) {
        if self.refreshing {
            self.refresh_again = Some(self.refresh_again.unwrap_or(false) || tell_client);
            return;
        }
        self.refreshing = true;
        self.refresh_on_notice = !wait.is_zero();
        let tx = self.events.clone();
        let log = self.log.clone();
        let path = self.manifest_path.clone();
        let served = self.served.clone();
        let name = self.name.clone();
        let launch = self.launch.clone();
        let discover = self.head.as_ref().and_then(|h| h.discover.clone());
        let _ = std::thread::Builder::new().name("refresh".into()).spawn(move || {
            let done = |notify: Vec<&'static str>, init: Option<Value>, changed: bool| {
                let _ = tx.send(Event::Refreshed {
                    gen,
                    notify,
                    init,
                    changed,
                });
            };
            if !wait.is_zero() {
                // Triggered by the server's list_changed: let a burst (one per tool it registers) settle.
                std::thread::sleep(wait);
            }
            let lists = match server.fetch_lists(Duration::from_secs(60)) {
                Ok(l) => l,
                Err(e) => {
                    log.warn(&format!("background list refresh failed: {e}"));
                    return done(Vec::new(), None, false);
                }
            };
            if server.is_dead() {
                return done(Vec::new(), None, false);
            }
            let init = lock(&server.init_result).clone().unwrap_or(Value::Null);
            let mut disk = manifest::read_full(&path).unwrap_or_else(|| {
                let empty = Lists {
                    tools: vec![],
                    prompts: vec![],
                    resources: vec![],
                    resource_templates: vec![],
                };
                let discover = discover.clone().or_else(|| server.probe("server/discover"));
                manifest::build_manifest(&name, &launch.command, &launch.args, &init, empty, discover)
            });
            let mut changed = false;
            let mut notify: Vec<&'static str> = Vec::new();
            for kind in KINDS {
                let disk_list: Vec<Value> = disk.get(kind).and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let live = lists.get(kind);
                if !manifest::lists_equal(kind, &disk_list, live) {
                    disk[kind] = Value::Array(live.clone());
                    changed = true;
                    notify.push(kind);
                } else if let Some(h) = served.get(kind) {
                    // The client was served an older copy (another session refreshed the file since).
                    if manifest::read_list(&path, kind).map(|(_, now)| now != *h).unwrap_or(false) {
                        notify.push(kind);
                    }
                }
            }
            let di = disk.get("initialize").cloned().unwrap_or(Value::Null);
            let same = di == manifest::init_block(&init);
            // `serve -- <command>` with a changed command: record it, so `serve` without one runs it too.
            let now_launch = json!({ "command": launch.command, "args": launch.args });
            if disk.get("launch") != Some(&now_launch) {
                disk["launch"] = now_launch;
                changed = true;
                log.info("manifest launch command updated");
            }
            if !same {
                disk["initialize"] = manifest::init_block(&init);
                changed = true;
            }
            if changed {
                disk["fetchedAt"] = json!(crate::timefmt::iso_now());
                if let Err(e) = manifest::write_manifest(&path, &disk) {
                    log.error(&format!("cannot write manifest: {e}"));
                }
                let what = if notify.is_empty() {
                    "serverInfo/capabilities".to_string()
                } else {
                    notify.join(", ")
                };
                log.info(&format!("manifest refreshed (changed: {what})"));
            }
            let notify = if tell_client { notify } else { Vec::new() };
            done(notify, if same { None } else { Some(manifest::init_block(&init)) }, changed);
        });
    }

    fn on_refreshed(&mut self, gen: u64, notify: Vec<&'static str>, init: Option<Value>, changed: bool) {
        self.refreshing = false;
        if self.refresh_on_notice {
            self.quiet_notices = if changed { 0 } else { (self.quiet_notices + 1).min(6) };
        }
        let again = self.refresh_again.take();
        let current = gen == self.gen;
        if let (true, Some(i), Some(h)) = (current, init, self.head.as_mut()) {
            h.initialize = i;
        }
        if current && changed {
            // The cached answers (after a crash, in this session) follow the rewritten manifest.
            if let Some(h) = manifest::read_head(&self.manifest_path) {
                self.head = Some(h);
            }
        }
        if current && self.client_initialized {
            let mut sent: Vec<&str> = Vec::new();
            for k in notify {
                let m = changed_method(k);
                if !sent.contains(&m) {
                    sent.push(m);
                    emit(&json!({ "jsonrpc": "2.0", "method": m }).to_string());
                }
            }
        }
        // Run what was asked for meanwhile, for the current server (a restart may have replaced the one this
        // refresh listed). A list_changed waits per notice_wait.
        if let (Some(tell), State::Ready, Some(server)) = (again, self.state, self.server.clone()) {
            let wait = if tell { Duration::ZERO } else { self.notice_wait() };
            self.refresh_in_background(server, self.gen, tell, wait);
        }
    }
}
