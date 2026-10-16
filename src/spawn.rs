// Process launching without a shell, and process-tree killing, for Windows and Unix.
//
// The working directory is never searched for a bare command name: clients start servers inside
// arbitrary repositories, so a planted `npx.cmd` (or `npx`) there must not stand in for the real one.
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

#[derive(Clone, Debug)]
pub struct Launch {
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
}

fn env_lookup(extra: &[(String, String)], key: &str) -> Option<OsString> {
    // The launch's own env wins over the inherited one. Windows names are case-insensitive.
    for (k, v) in extra.iter().rev() {
        let same = if cfg!(windows) { k.eq_ignore_ascii_case(key) } else { k == key };
        if same {
            return Some(OsString::from(v));
        }
    }
    std::env::var_os(key)
}

fn base_dir(launch: &Launch) -> PathBuf {
    launch
        .cwd
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_default()
}

fn first_hit(path: &OsString, candidates: impl Fn(PathBuf) -> Vec<PathBuf>, ok: impl Fn(&Path) -> bool) -> Option<PathBuf> {
    for d in std::env::split_paths(path) {
        // An empty or relative entry ("." included) would search the working directory.
        if !d.is_absolute() {
            continue;
        }
        if let Some(hit) = candidates(d).into_iter().find(|p| ok(p)) {
            return Some(hit);
        }
    }
    None
}

#[cfg(windows)]
mod os {
    use super::*;
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    fn is_file(p: &Path) -> bool {
        p.metadata().map(|m| m.is_file()).unwrap_or(false)
    }

    /// Resolve a command to a file, honouring PATH and PATHEXT.
    pub fn resolve_command(command: &str, launch: &Launch) -> Option<PathBuf> {
        let pathext = env_lookup(&launch.env, "PATHEXT")
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".into());
        let exts: Vec<String> = pathext.split(';').filter(|e| !e.is_empty()).map(|e| e.to_string()).collect();
        let file_name = Path::new(command)
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default();
        let has_ext = Path::new(&file_name).extension().map(|e| !e.is_empty()).unwrap_or(false) && !file_name.starts_with('.');
        let candidates = |dir: PathBuf| -> Vec<PathBuf> {
            let base = dir.join(command);
            let mut v = Vec::new();
            if has_ext {
                v.push(base.clone());
            }
            for e in &exts {
                let mut s = base.clone().into_os_string();
                s.push(e);
                v.push(PathBuf::from(s));
            }
            v
        };
        if command.contains('\\') || command.contains('/') || Path::new(command).is_absolute() {
            return candidates(base_dir(launch)).into_iter().find(|p| is_file(p));
        }
        first_hit(&env_lookup(&launch.env, "PATH")?, candidates, is_file)
    }

    /// cross-spawn style escaping of one argument for `cmd.exe /d /s /c "..."`. `parses`: how many times
    /// cmd.exe reads it: once for the batch file's own path, twice for its arguments (the batch file
    /// re-reads them when it expands `%*` or `%1`).
    fn escape_for_cmd(arg: &str, parses: usize) -> String {
        // 1. Double the backslashes in front of each quote, then escape the quote: (\*)" -> $1$1\"
        let mut a = String::with_capacity(arg.len() + 8);
        let mut slashes = 0usize;
        for ch in arg.chars() {
            if ch == '\\' {
                slashes += 1;
                continue;
            }
            if ch == '"' {
                a.push_str(&"\\".repeat(slashes * 2));
                a.push_str("\\\"");
            } else {
                a.push_str(&"\\".repeat(slashes));
                a.push(ch);
            }
            slashes = 0;
        }
        // 2. Double the trailing backslashes: (\*)$ -> $1$1
        a.push_str(&"\\".repeat(slashes * 2));
        // 3. Quote, then caret-escape cmd metacharacters (the quotes included).
        let mut out = format!("\"{a}\"");
        for _ in 0..parses {
            let mut next = String::with_capacity(out.len() * 2);
            for ch in out.chars() {
                if "()][%!^\"`<>&|;, *?".contains(ch) {
                    next.push('^');
                }
                next.push(ch);
            }
            out = next;
        }
        out
    }

    pub fn command_for(file: &Path, launch: &Launch) -> Command {
        let ext = file
            .extension()
            .map(|e| e.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        let mut cmd = if ext == "cmd" || ext == "bat" {
            // Batch files only run through cmd.exe; quote every argument so none is re-parsed by it.
            let comspec = env_lookup(&launch.env, "ComSpec")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(r"C:\Windows\System32\cmd.exe"));
            let mut line = escape_for_cmd(&file.to_string_lossy(), 1);
            for a in &launch.args {
                line.push(' ');
                line.push_str(&escape_for_cmd(a, 2));
            }
            let mut c = Command::new(comspec);
            c.raw_arg("/d").raw_arg("/s").raw_arg("/c").raw_arg(format!("\"{line}\""));
            c
        } else {
            let mut c = Command::new(file);
            c.args(&launch.args);
            c
        };
        // The server gets a hidden console of its own instead of a visible window.
        cmd.creation_flags(CREATE_NO_WINDOW);
        cmd
    }

    /// Kill a process and all of its descendants.
    pub fn kill_tree(pid: u32) {
        let root = std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
        let _ = Command::new(root.join("System32").join("taskkill.exe"))
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .status();
    }
}

#[cfg(unix)]
mod os {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;

    fn is_executable(p: &Path) -> bool {
        p.metadata()
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }

    /// Resolve a command to an executable file through PATH.
    pub fn resolve_command(command: &str, launch: &Launch) -> Option<PathBuf> {
        if command.contains('/') {
            let p = base_dir(launch).join(command);
            return is_executable(&p).then_some(p);
        }
        first_hit(&env_lookup(&launch.env, "PATH")?, |dir| vec![dir.join(command)], is_executable)
    }

    pub fn command_for(file: &Path, launch: &Launch) -> Command {
        let mut c = Command::new(file);
        c.args(&launch.args);
        // Its own process group, so the whole tree (npx -> node, uv -> python) can be signalled at once.
        c.process_group(0);
        c
    }

    /// Kill the server's process group: SIGTERM, then SIGKILL for anything still there after 2 s.
    pub fn kill_tree(pid: u32) {
        let Ok(pgid) = libc::pid_t::try_from(pid) else { return };
        if pgid <= 1 {
            return;
        }
        // SAFETY: plain syscalls on a process group this proxy created (process_group(0) above).
        unsafe {
            if libc::kill(-pgid, libc::SIGTERM) != 0 {
                return; // no such group left
            }
        }
        for _ in 0..40 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            // Signal 0 only checks whether any member of the group still exists.
            if unsafe { libc::kill(-pgid, 0) } != 0 {
                return;
            }
        }
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
}

pub use os::{kill_tree, resolve_command};

pub fn start_process(launch: &Launch) -> Result<Child, String> {
    let searched = if cfg!(windows) {
        "PATH/PATHEXT searched"
    } else {
        "PATH searched, executable files only"
    };
    let file =
        resolve_command(&launch.command, launch).ok_or_else(|| format!("cannot resolve command \"{}\" ({searched})", launch.command))?;
    let mut cmd = os::command_for(&file, launch);
    cmd.envs(launch.env.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    if let Some(d) = &launch.cwd {
        cmd.current_dir(d);
    }
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    cmd.spawn().map_err(|e| format!("spawn {}: {e}", file.display()))
}
