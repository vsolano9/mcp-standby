# mcp-standby

Keep MCP servers on standby. Your client sees every tool, prompt and resource a server offers, but the server process only starts when a request actually needs it.

Every agent session (Claude Code, Codex and other MCP clients) normally starts every configured stdio server up front, whether the session uses it or not. With a handful of servers and several sessions open, that is a lot of idle Node, Python and JVM processes. `mcp-standby` sits in front of one server, answers discovery from a cached manifest, and starts the real server on the first call.

```
client ──stdio──> mcp-standby ──(first tools/call)──> real server
                    │
                    └─ initialize, tools/list, prompts/list, resources/list,
                       ping: answered from the manifest, no server running
```

Measured on Windows 11 with the reference [`@modelcontextprotocol/server-everything`](https://github.com/modelcontextprotocol/servers) 1.0.0 (Node 24), over repeated runs: private memory of the whole process tree after `initialize` and `tools/list` and 3 s idle, and for the last row after one `tools/call` and 3 s idle:

| | Memory | Processes |
|---|---|---|
| Server started directly | 47 to 50 MB | `node` |
| Behind `mcp-standby`, before first use | under 1 MB | `mcp-standby` |
| Behind `mcp-standby`, after first use | 48 to 51 MB | `mcp-standby` about 1 MB + `node` |

The first `tools/call` takes a few hundred milliseconds (210 to 480 ms over our runs; it starts the server and does the MCP handshake); the next ones take 1 to 2 ms. Heavier servers (Python ML stacks, browser automation, Ghidra-style tools) save correspondingly more. Your numbers depend on the server.

## Install

Download the archive for your platform from the [latest release](https://github.com/vsolano9/mcp-standby/releases/latest), check it against `SHA256SUMS`, and put the `mcp-standby` binary on your `PATH`.

| Platform | Archive |
|---|---|
| Windows x64 | `mcp-standby-v1.0.0-x86_64-pc-windows-msvc.zip` |
| Linux x64 (static, any distribution) | `mcp-standby-v1.0.0-x86_64-unknown-linux-musl.tar.gz` |
| macOS 11+ Apple silicon | `mcp-standby-v1.0.0-aarch64-apple-darwin.tar.gz` |

Or build it (Rust 1.85+):

```sh
cargo install --git https://github.com/vsolano9/mcp-standby --locked
```

It is a single binary of under 1 MB with no runtime dependencies.

## Use

Take the command your client runs for a server and put `mcp-standby serve --name <name> --` in front of it.

**Claude Code**

```sh
claude mcp add-json filesystem '{"type":"stdio","command":"mcp-standby","args":["serve","--name","filesystem","--","npx","-y","@modelcontextprotocol/server-filesystem","/home/me/projects"]}'
```

**Codex** (`~/.codex/config.toml`)

```toml
[mcp_servers.filesystem]
command = "mcp-standby"
args = ["serve", "--name", "filesystem", "--", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/home/me/projects"]
```

**Any client with a JSON `mcpServers` block**

```json
{
  "mcpServers": {
    "filesystem": {
      "command": "mcp-standby",
      "args": ["serve", "--name", "filesystem", "--", "npx", "-y", "@modelcontextprotocol/server-filesystem", "/home/me/projects"],
      "env": { "SOME_TOKEN": "..." }
    }
  }
}
```

The `env` block still reaches the real server: it inherits the proxy's environment.

The first session without a manifest starts the server right away, records what it offers, and from then on it waits on standby. To record the manifest ahead of time (for example after upgrading a server):

```sh
mcp-standby refresh --name filesystem -- npx -y @modelcontextprotocol/server-filesystem /home/me/projects
```

Verified as a client: Claude Code 2.1.301 on Windows (a session that used no tool from the server never started it; a tool call started it and returned the server's answer; ending the session stopped it). Other clients speak the same stdio protocol but have not been checked here.

## Commands and options

```
mcp-standby serve   --name <name> [options] -- <command> [args...]
mcp-standby refresh --name <name> [options] -- <command> [args...]
```

| Option | Meaning |
|---|---|
| `--name <name>` | Server name for the default manifest path and the log file |
| `--manifest <file>` | Manifest path instead of `<data dir>/manifests/<name>.json` (the name then defaults to the file name) |
| `--env KEY=VALUE` | Extra environment for the real server (repeatable) |
| `--cwd <dir>` | Working directory for the real server |
| `--startup-timeout <ms>` | How long the real server gets for its `initialize` answer (default 120000) |
| `--no-log-file` | Log to stderr only |

`serve` without a command after `--` uses the command recorded in the manifest.

The data directory is `MCP_STANDBY_HOME` if set, else `%LOCALAPPDATA%\mcp-standby` on Windows, else `$XDG_DATA_HOME/mcp-standby` or `~/.local/share/mcp-standby`. Logs go to stderr and `<data dir>/logs/<name>.log` (1 MB, 3 rotations); `MCP_STANDBY_DEBUG=1` adds debug lines.

## How it works

- **Answered from the manifest, no server:** `initialize` (cached `serverInfo`, `instructions` and `capabilities`, with `listChanged` added; protocol versions 2025-11-25, 2025-06-18, 2025-03-26 and 2024-11-05: the version the server agreed on when asked for the newest one, or the one it agreed on for the version this client asks for in an earlier session; a client asking for an older version the manifest has no answer for starts the server once to negotiate, and the manifest remembers the answer), `ping`, `tools/list`, `prompts/list`, `resources/list`, `resources/templates/list`, and the `server/discover` probe some clients send before `initialize`.
- **Starts the server:** anything else, such as `tools/call`, `resources/read`, `prompts/get` or `completion/complete`. Requests and notifications that arrive while it starts are queued (a queued request the client cancels is dropped). The proxy sends the server the client's original `initialize` parameters, then forwards everything in order. Client request ids pass through untouched (ids under the proxy's own prefix are refused); server-initiated requests (sampling, roots, elicitation) reach the client under ids of the proxy's own and the answers go back under the server's ids, so an answer that arrives after a restart is dropped instead of reaching the new server. Notifications, progress and cancellations flow both ways, the proxy answers the server's `ping` itself, and the server's own messages reach the client only after it has its `initialize` answer.
- **Once the server runs,** list requests go to it, so the client always sees its current lists. After a deferred start the proxy compares the live lists, `initialize` result and launch command with the manifest in the background; when they differ it rewrites the manifest (atomically) and sends the client `list_changed` for the lists that changed. When the server itself sends `list_changed`, the client gets it and, after a short wait, the manifest is updated for the next session (one refresh at a time; the wait doubles from 1 s up to 60 s while announcements keep coming and starts over after a quiet minute, so a server that announces a change every time it is listed costs one listing a minute).
- **Shutdown:** when the client closes stdin (and on SIGTERM, SIGINT or SIGHUP on Unix) the proxy stops the server and the processes it started: `taskkill /T /F` on the server's process tree on Windows, SIGTERM then (after 2 s) SIGKILL to the server's process group on Unix.

| Situation | Behavior |
|---|---|
| No manifest, or an unreadable one | The server starts at `initialize`, the manifest is written, then normal operation |
| Server fails to start or does not answer `initialize` in time | Queued requests get JSON-RPC error `-32603`; the next request tries again |
| Server dies mid-session | In-flight requests get `-32603`; the next request starts it again with the original `initialize` parameters (server-side session state is lost, as with any restart) |
| Command not found | Same as a failed start, with the reason in the error message |
| Client sends a JSON-RPC batch | Handled element by element |

Commands are found through the absolute entries of `PATH` (and `PATHEXT` on Windows, where `.cmd` and `.bat` launchers such as `npx.cmd` run through `cmd.exe` with each argument quoted and escaped). The current directory is never searched for a bare command name; a command given as a relative path is resolved from `--cwd`. On Windows the proxy is a GUI-subsystem program, so it does not get a console window of its own; the server gets a hidden one.

## Limits

- It saves memory and process count, not context tokens: the client still sees every tool. If your problem is tool definitions filling the context window, look at your client's tool search or a gateway that exposes tools on demand.
- The manifest is a snapshot taken when the server last ran. It is corrected the first time the server runs in a later session, not before.
- Protocol versions are cached per server: the manifest tells a changed server by its `initialize` answer (name, version, capabilities, instructions) and its command. A server replaced in place under the same name, version and command keeps its recorded versions until a client asking for the newest version uses it. A client told a recorded version that the new server answers differently keeps that version for its session; the manifest is corrected for the next one.
- There is no idle shutdown: once started, the server runs until the session ends (many servers keep session state).
- stdio servers only; no HTTP or SSE transport.
- If the proxy itself is killed hard (`kill -9`, `taskkill /F` on it), it cannot stop the server. The server sees its stdin close, and most servers exit then.
- A server that exits on its own can leave processes it started behind; the proxy only signals a server that is still running, so it never hits a reused process ID. Processes that detach (a new session or process group on Unix, or a child whose parent already exited on Windows) are outside the tree the proxy stops. A process left behind that keeps the server's output open also keeps one of the proxy's reader threads waiting until it exits.
- Servers whose lists depend on the client (for example tools offered only to clients with elicitation) are recorded as the last client saw them.
- Manifests record the launch command and its arguments, never the environment. Pass secrets through environment variables, not arguments. See [SECURITY.md](SECURITY.md).

## Related projects

Starting MCP servers on demand is not a new idea. [tkukushkin/lazy-mcp](https://github.com/tkukushkin/lazy-mcp) also caches discovery and starts the server on first use. Projects such as [voicetreelab/lazy-mcp](https://github.com/voicetreelab/lazy-mcp) and [PeterCha90/mcp-lazy-load](https://github.com/PeterCha90/mcp-lazy-load) aim at context size instead and expose tools through a search or activation step. `mcp-standby` keeps the server's own discovery unchanged, is one native binary per server entry, and keeps its manifest in step with the server.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

[MIT](LICENSE)
