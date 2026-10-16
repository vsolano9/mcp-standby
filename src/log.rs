// Logging: stderr always (stdout is reserved for protocol), plus an optional size-rotated file under
// <data root>/logs/<name>.log (1 MB, 3 rotations).
//
// stderr goes through its own thread and a bounded queue: a client that stops draining our stderr
// pipe must never block the proxy, so lines are dropped instead.
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Mutex;

use crate::timefmt::iso_now;

const MAX_BYTES: u64 = 1024 * 1024;
const KEEP: u32 = 3;

/// Where manifests and logs live: MCP_STANDBY_HOME, else %LOCALAPPDATA%\mcp-standby on Windows,
/// else $XDG_DATA_HOME/mcp-standby, else ~/.local/share/mcp-standby.
pub fn data_root() -> PathBuf {
    if let Some(h) = std::env::var_os("MCP_STANDBY_HOME").filter(|h| !h.is_empty()) {
        return PathBuf::from(h);
    }
    if cfg!(windows) {
        if let Some(l) = std::env::var_os("LOCALAPPDATA").filter(|l| !l.is_empty()) {
            return PathBuf::from(l).join("mcp-standby");
        }
    }
    if let Some(x) = std::env::var_os("XDG_DATA_HOME").filter(|x| !x.is_empty()) {
        return PathBuf::from(x).join("mcp-standby");
    }
    let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).unwrap_or_default();
    PathBuf::from(home).join(".local").join("share").join("mcp-standby")
}

struct FileState {
    file: Option<File>,
    size: u64,
    path: PathBuf,
}

pub struct Logger {
    name: String,
    debug: bool,
    pid: u32,
    file: Mutex<FileState>,
    stderr: SyncSender<Vec<u8>>,
}

fn rotate(path: &PathBuf) {
    match fs::metadata(path) {
        Ok(m) if m.len() >= MAX_BYTES => {}
        _ => return,
    }
    for i in (1..KEEP).rev() {
        let from = PathBuf::from(format!("{}.{}", path.display(), i));
        if from.exists() {
            let _ = fs::rename(&from, format!("{}.{}", path.display(), i + 1));
        }
    }
    let _ = fs::rename(path, format!("{}.1", path.display()));
}

fn open_append(path: &PathBuf) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

impl Logger {
    pub fn new(name: &str, file: bool) -> Logger {
        let (tx, rx) = sync_channel::<Vec<u8>>(256);
        std::thread::Builder::new()
            .name("stderr".into())
            .spawn(move || {
                let mut err = std::io::stderr();
                for chunk in rx {
                    if err.write_all(&chunk).is_err() {
                        // stderr is gone: keep draining so senders never block.
                        continue;
                    }
                    let _ = err.flush();
                }
            })
            .ok();
        let mut st = FileState {
            file: None,
            size: 0,
            path: PathBuf::new(),
        };
        if file {
            let dir = data_root().join("logs");
            if fs::create_dir_all(&dir).is_ok() {
                st.path = dir.join(format!("{name}.log"));
                rotate(&st.path);
                st.file = open_append(&st.path);
                st.size = st.file.as_ref().and_then(|f| f.metadata().ok()).map(|m| m.len()).unwrap_or(0);
            }
        }
        Logger {
            name: name.to_string(),
            debug: std::env::var("MCP_STANDBY_DEBUG").map(|v| v == "1").unwrap_or(false),
            pid: std::process::id(),
            file: Mutex::new(st),
            stderr: tx,
        }
    }

    fn to_stderr(&self, bytes: Vec<u8>) {
        let _ = self.stderr.try_send(bytes);
    }

    fn to_file(&self, bytes: &[u8]) {
        let mut st = match self.file.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let Some(f) = st.file.as_mut() else { return };
        if f.write_all(bytes).is_err() {
            st.file = None;
            return;
        }
        st.size += bytes.len() as u64;
        if st.size > MAX_BYTES {
            st.file = None;
            let path = st.path.clone();
            rotate(&path);
            st.file = open_append(&path);
            st.size = 0;
        }
    }

    fn write(&self, level: &str, msg: &str) {
        self.to_stderr(format!("[mcp-standby:{}] {} {}\n", self.name, level, msg).into_bytes());
        self.to_file(format!("{} [{}] pid={} {}\n", iso_now(), level, self.pid, msg).as_bytes());
    }

    pub fn info(&self, m: &str) {
        self.write("info", m)
    }
    pub fn warn(&self, m: &str) {
        self.write("warn", m)
    }
    pub fn error(&self, m: &str) {
        self.write("error", m)
    }
    pub fn debug(&self, m: &str) {
        if self.debug {
            self.write("debug", m)
        }
    }
    // Child stderr passthrough.
    pub fn raw(&self, bytes: &[u8]) {
        self.to_stderr(bytes.to_vec());
        self.to_file(bytes);
    }
}
