// mcp-standby: a stdio MCP proxy that answers discovery from a cached manifest and starts the real
// server only when a request needs it.
//
// Windows GUI subsystem on purpose: an MCP client spawns this with piped stdio, and a console-subsystem
// program would get its own conhost.exe for nothing. The real server still gets a hidden console of
// its own (CREATE_NO_WINDOW). The attribute does nothing on other platforms.
#![windows_subsystem = "windows"]

mod log;
mod manifest;
mod proxy;
mod real;
mod spawn;
mod timefmt;

use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde_json::json;

use crate::log::Logger;
use crate::proxy::{Event, Proxy, Shared, SUPPORTED_VERSIONS, VERSION};
use crate::real::{RealServer, Sink};
use crate::spawn::Launch;

static SHARED: OnceLock<Arc<Mutex<Shared>>> = OnceLock::new();
static LOG: OnceLock<Arc<Logger>> = OnceLock::new();
// Set while `refresh` runs: an interrupted refresh exits non-zero.
static REFRESHING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static PANICKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn help() -> String {
    format!(
        "mcp-standby {VERSION}

Keeps an MCP server on standby: discovery is answered from a cached manifest, and the real
server starts on the first request that needs it.

Usage:
  mcp-standby serve   (--name <name> | --manifest <file>) [options] [-- <command> [args...]]
  mcp-standby refresh (--name <name> | --manifest <file>) [options] -- <command> [args...]

  refresh  start the server once, record initialize and every list into the manifest, stop it
  serve    what an MCP client launches (stdio)

Options:
  --manifest <file>        Manifest path (default: <data dir>/manifests/<name>.json)
  --name <name>            Server name for logs and the default manifest (default: manifest file name)
  --env KEY=VALUE          Extra environment for the real server (repeatable)
  --cwd <dir>              Working directory for the real server
  --startup-timeout <ms>   Handshake timeout for the real server (default 120000)
  --no-log-file            Do not write <data dir>/logs/<name>.log
  -h, --help | -v, --version

Data dir: {}
",
        log::data_root().display()
    )
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn AttachConsole(pid: u32) -> i32;
    fn GetStdHandle(which: u32) -> isize;
}

// A GUI-subsystem program run by hand from a terminal has no stdout/stderr; borrow the parent's
// console for help, version, errors and refresh output. Never in `serve` with piped stdio.
#[cfg(windows)]
fn attach_console_if_needed() {
    const STD_ERROR_HANDLE: u32 = -12i32 as u32;
    // SAFETY: plain Win32 calls without pointers.
    unsafe {
        let h = GetStdHandle(STD_ERROR_HANDLE);
        if h == 0 || h == -1 {
            AttachConsole(u32::MAX);
        }
    }
}

#[cfg(not(windows))]
fn attach_console_if_needed() {}

// On Unix a client may stop the proxy with SIGTERM, SIGINT or SIGHUP instead of closing stdin; run the
// same shutdown (kill the server's process group) for those. The handler only writes one byte to a
// pipe; a thread reading the pipe does the work. Windows clients close stdin or kill the process.
#[cfg(unix)]
fn watch_signals() {
    static PIPE_W: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);
    extern "C" fn on_signal(_: libc::c_int) {
        let fd = PIPE_W.load(std::sync::atomic::Ordering::Relaxed);
        if fd >= 0 {
            // SAFETY: write(2) is async-signal-safe; the byte's value does not matter.
            unsafe {
                libc::write(fd, b"s".as_ptr().cast(), 1);
            }
        }
    }
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: pipe(2) fills the two-element array.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return;
    }
    for fd in fds {
        // SAFETY: mark both ends close-on-exec so the real server never inherits them.
        unsafe {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
    PIPE_W.store(fds[1], std::sync::atomic::Ordering::Relaxed);
    let read_fd = fds[0];
    let started = std::thread::Builder::new().name("signals".into()).spawn(move || {
        let mut b = [0u8; 1];
        // SAFETY: read(2) into a one-byte buffer.
        while unsafe { libc::read(read_fd, b.as_mut_ptr().cast(), 1) } < 0 {}
        stop("signal received");
    });
    if started.is_err() {
        return;
    }
    for sig in [libc::SIGTERM, libc::SIGINT, libc::SIGHUP] {
        // SAFETY: installs an async-signal-safe handler (see on_signal).
        unsafe {
            libc::signal(sig, on_signal as extern "C" fn(libc::c_int) as libc::sighandler_t);
        }
    }
}

#[cfg(not(unix))]
fn watch_signals() {}

/// Shut down: kill the real server tree and exit (stdin closed, stdout gone, signal, uncaught panic).
/// `serve` exits 0; an interrupted `refresh` exits 1.
pub fn stop(why: &str) -> ! {
    if let Some(l) = LOG.get() {
        l.info(&format!("stopping: {why}"));
    }
    if let Some(sh) = SHARED.get() {
        // The starter holds the lock while it creates the real server and records it; wait for that (up to
        // 5 s, for a slow CreateProcess) so the server it is starting is killed instead of left running. Never
        // a blocking lock(): stop() also runs from the panic hook, possibly on a thread that holds it.
        let mut guard = None;
        for _ in 0..250 {
            match sh.try_lock() {
                Ok(g) => {
                    guard = Some(g);
                    break;
                }
                Err(std::sync::TryLockError::Poisoned(p)) => {
                    guard = Some(p.into_inner());
                    break;
                }
                Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        if guard.is_none() {
            if let Some(l) = LOG.get() {
                l.error("shutdown: the starter kept the lock for 5 s; exiting without killing the server");
            }
        }
        if let Some(mut g) = guard {
            g.shutting_down = true;
            if let Some(s) = g.server.take() {
                if let Some(l) = LOG.get() {
                    l.info("shutdown: killing server process tree");
                }
                s.kill();
            }
        }
    }
    let failed = REFRESHING.load(std::sync::atomic::Ordering::SeqCst) || PANICKED.load(std::sync::atomic::Ordering::SeqCst);
    std::process::exit(if failed { 1 } else { 0 });
}

struct Opts {
    cmd: String,
    manifest: Option<String>,
    name: Option<String>,
    env: Vec<(String, String)>,
    cwd: Option<String>,
    startup_timeout_ms: u64,
    log_file: bool,
    command: Option<String>,
    args: Vec<String>,
}

fn parse(argv: &[String]) -> Result<Opts, String> {
    let sep = argv.iter().position(|a| a == "--");
    let (head, tail) = match sep {
        Some(i) => (&argv[..i], &argv[i + 1..]),
        None => (argv, &argv[argv.len()..]),
    };
    let mut o = Opts {
        cmd: head.first().cloned().unwrap_or_default(),
        manifest: None,
        name: None,
        env: Vec::new(),
        cwd: None,
        startup_timeout_ms: 120_000,
        log_file: true,
        command: tail.first().cloned(),
        args: tail.iter().skip(1).cloned().collect(),
    };
    let mut i = 1;
    while i < head.len() {
        let a = head[i].as_str();
        let mut val = || -> Result<String, String> {
            i += 1;
            head.get(i).cloned().ok_or_else(|| format!("{a} needs a value"))
        };
        match a {
            "--manifest" => o.manifest = Some(val()?),
            "--name" => o.name = Some(val()?),
            "--cwd" => o.cwd = Some(val()?),
            "--startup-timeout" => {
                let v = val()?;
                o.startup_timeout_ms = v
                    .parse()
                    .map_err(|_| format!("--startup-timeout expects milliseconds, got \"{v}\""))?;
            }
            "--no-log-file" => o.log_file = false,
            "--env" => {
                let kv = val()?;
                match kv.find('=') {
                    Some(eq) if eq >= 1 => o.env.push((kv[..eq].to_string(), kv[eq + 1..].to_string())),
                    _ => return Err(format!("--env expects KEY=VALUE, got \"{kv}\"")),
                }
            }
            other => return Err(format!("unknown option {other}")),
        }
        i += 1;
    }
    Ok(o)
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let first = argv.first().map(|s| s.as_str()).unwrap_or("");
    if argv.is_empty() || first == "-h" || first == "--help" {
        attach_console_if_needed();
        print!("{}", help());
        return;
    }
    if first == "-v" || first == "--version" {
        attach_console_if_needed();
        println!("{VERSION}");
        return;
    }
    let o = match parse(&argv) {
        Ok(o) => o,
        Err(e) => {
            attach_console_if_needed();
            eprintln!("mcp-standby: {e}");
            std::process::exit(2);
        }
    };
    if o.cmd != "serve" && o.cmd != "refresh" {
        attach_console_if_needed();
        eprint!("mcp-standby: unknown command \"{}\"\n{}", o.cmd, help());
        std::process::exit(2);
    }
    if o.cmd == "refresh" {
        attach_console_if_needed();
    }
    let name = o.name.clone().or_else(|| {
        o.manifest.as_ref().map(|m| {
            let base = Path::new(m)
                .file_name()
                .map(|f| f.to_string_lossy().into_owned())
                .unwrap_or_default();
            if base.to_ascii_lowercase().ends_with(".json") {
                base[..base.len() - 5].to_string()
            } else {
                base
            }
        })
    });
    let Some(name) = name else {
        eprintln!("mcp-standby: --manifest or --name is required");
        std::process::exit(2);
    };
    let manifest_path = match &o.manifest {
        Some(m) => std::path::absolute(m).unwrap_or_else(|_| PathBuf::from(m)),
        None => manifest::default_manifest_path(&name),
    };
    let launch = match &o.command {
        Some(c) => Launch {
            command: c.clone(),
            args: o.args.clone(),
            env: o.env.clone(),
            cwd: o.cwd.clone(),
        },
        None => {
            let l = manifest::read_head(&manifest_path).and_then(|h| h.launch);
            let cmd = l
                .as_ref()
                .and_then(|l| l.get("command"))
                .and_then(|c| c.as_str())
                .map(|s| s.to_string());
            let Some(cmd) = cmd else {
                eprintln!("mcp-standby: no command after \"--\" and no launch info in the manifest");
                std::process::exit(2);
            };
            let args = l
                .as_ref()
                .and_then(|l| l.get("args"))
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            Launch {
                command: cmd,
                args,
                env: o.env.clone(),
                cwd: o.cwd.clone(),
            }
        }
    };
    let log = Arc::new(Logger::new(&name, o.log_file));
    let _ = LOG.set(log.clone());
    let timeout = Duration::from_millis(o.startup_timeout_ms);
    if o.cmd == "refresh" {
        std::process::exit(refresh(&name, &manifest_path, &launch, log, timeout));
    }
    serve(name, manifest_path, launch, log, timeout);
}

fn refresh(name: &str, manifest_path: &Path, launch: &Launch, log: Arc<Logger>, timeout: Duration) -> i32 {
    let sink: Sink = Arc::new(|_| {});
    REFRESHING.store(true, std::sync::atomic::Ordering::SeqCst);
    // Ctrl+C during a refresh must not leave the server (in its own process group on Unix) running: the
    // signal watcher is up before the spawn, and the lock it waits for is held until the server is recorded.
    let shared = Arc::new(Mutex::new(Shared {
        shutting_down: false,
        server: None,
    }));
    let _ = SHARED.set(shared.clone());
    watch_signals();
    let server = {
        let mut g = shared.lock().unwrap_or_else(|p| p.into_inner());
        match RealServer::spawn(launch, log, 1, sink) {
            Ok(s) => {
                g.server = Some(s.clone());
                s
            }
            Err(e) => {
                drop(g);
                eprintln!("mcp-standby: refresh failed: {e}");
                return 1;
            }
        }
    };
    let run = || -> Result<String, String> {
        let init = server
            .handshake(
                json!({ "protocolVersion": SUPPORTED_VERSIONS[0], "capabilities": {}, "clientInfo": { "name": "mcp-standby-refresh", "version": VERSION } }),
                timeout,
            )
            .map_err(|e| e.to_string())?;
        let lists = server.fetch_lists(Duration::from_secs(60)).map_err(|e| e.to_string())?;
        let discover = server.probe("server/discover");
        let counts = (
            lists.tools.len(),
            lists.prompts.len(),
            lists.resources.len(),
            lists.resource_templates.len(),
        );
        let m = manifest::build_manifest(name, &launch.command, &launch.args, &init, lists, discover);
        manifest::write_manifest(manifest_path, &m)?;
        let si = init.get("serverInfo");
        let s = |k: &str| si.and_then(|v| v.get(k)).and_then(|v| v.as_str()).unwrap_or("?").to_string();
        Ok(format!(
            "mcp-standby: wrote {} ({} tools, {} prompts, {} resources, {} templates; server {} {})",
            manifest_path.display(),
            counts.0,
            counts.1,
            counts.2,
            counts.3,
            s("name"),
            s("version")
        ))
    };
    let code = match run() {
        Ok(msg) => {
            eprintln!("{msg}");
            0
        }
        Err(e) => {
            eprintln!("mcp-standby: refresh failed: {e}");
            1
        }
    };
    server.kill();
    code
}

fn serve(name: String, manifest_path: PathBuf, launch: Launch, log: Arc<Logger>, timeout: Duration) {
    let shared = Arc::new(Mutex::new(Shared {
        shutting_down: false,
        server: None,
    }));
    let _ = SHARED.set(shared.clone());
    watch_signals();
    std::panic::set_hook(Box::new(|info| {
        if let Some(l) = LOG.get() {
            l.error(&format!("uncaught: {info}"));
        }
        PANICKED.store(true, std::sync::atomic::Ordering::SeqCst);
        stop("uncaught exception");
    }));
    let (tx, rx) = channel::<Event>();
    let stdin_tx = tx.clone();
    let reader = std::thread::Builder::new().name("client-stdin".into()).spawn(move || {
        let stdin = std::io::stdin();
        let mut lock = stdin.lock();
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match lock.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let mut end = buf.len();
                    while end > 0 && (buf[end - 1] == b'\n' || buf[end - 1] == b'\r') {
                        end -= 1;
                    }
                    if stdin_tx.send(Event::Client(buf[..end].to_vec())).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = stdin_tx.send(Event::ClientEof);
    });
    if reader.is_err() {
        log.error("cannot start the stdin reader");
        std::process::exit(1);
    }
    log.info(&format!("serving {} (manifest {})", name, manifest_path.display()));
    let mut proxy = Proxy::new(name, manifest_path, launch, log, timeout, tx, shared);
    for ev in rx {
        if let Event::ClientEof = ev {
            stop("stdin closed");
        }
        proxy.on_event(ev);
    }
    stop("event channel closed");
}
