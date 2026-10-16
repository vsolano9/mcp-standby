# Contributing

Issues and pull requests are welcome. Keep changes focused on what the proxy promises: a client sees the server's own tools, prompts, resources and answers, and the server does not run until a request needs it.

## Set up

Rust 1.80 or later. There is one binary and no runtime dependencies.

```sh
git clone https://github.com/vsolano9/mcp-standby.git
cd mcp-standby
cargo build --release
```

## Verify a change

```sh
cargo fmt --check
cargo clippy --release -- -D warnings
cargo build --release
```

Then exercise the real path against a real MCP server, on each OS your change touches. At minimum, run `refresh`, then `serve` with a client (or a scripted one) and check that

- `initialize` and `tools/list` are answered while no server process exists,
- the first `tools/call` starts the server and gets its answer,
- closing the client's stdin removes the server and the processes it started that are still attached to it,
- after the server changes a list and sends `list_changed`, the client gets the notification, `tools/list` returns the new list, and the manifest is rewritten.

For lifecycle changes also check a server that exits on its own, a command that cannot be resolved, a server that never answers `initialize` (`--startup-timeout`), and on Unix SIGTERM to the proxy. Check process cleanup on the OS itself; Windows (`taskkill /T`) and Unix (process groups) work differently.

## Pull requests

- Explain the problem, the behavior after the change, and compatibility (manifest schema, options, log format).
- Add a user-facing entry under `Unreleased` in `CHANGELOG.md` and update the README when behavior or options change.
- State exactly which checks you ran and on which OS. Do not describe a check you did not run as passed.
- Keep manifests, logs, local paths and credentials out of commits.
- Check the license of any dependency or borrowed code; the project is MIT.

There is no CI workflow; validation runs locally. Please do not add workflows as part of an unrelated change.
