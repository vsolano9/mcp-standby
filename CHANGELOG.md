# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-10-16

First public release.

### Added

- `serve`: a stdio MCP proxy that answers `initialize`, `ping`, the list methods and `server/discover` from a cached manifest and starts the real server on the first request that needs it, queueing messages during the start.
- `refresh`: records a server's `initialize` result and every list (following pagination cursors) into a manifest.
- Live lists once the server runs; background comparison with the manifest after a deferred start, with an atomic rewrite and `list_changed` to the client when they differ; manifest update (debounced, one refresh at a time) when the server sends `list_changed` itself.
- Server-initiated requests and notifications are held until the client has its `initialize` answer and has sent `notifications/initialized`; client notifications sent during a start are queued.
- Restart on the next request after a server crash or a failed start, with `-32603` errors for affected requests.
- Process-tree shutdown on stdin close: `taskkill /T /F` on Windows, SIGTERM then SIGKILL to the server's process group on Unix, also on SIGTERM, SIGINT and SIGHUP to the proxy.
- Command resolution through the absolute entries of `PATH` (and `PATHEXT` on Windows) without searching the current directory; `.cmd`/`.bat` launchers run through `cmd.exe` with arguments escaped for both of its parsing passes.
- Logs to stderr and a rotated file; launch arguments are not logged.
- Release builds for Windows x64, Linux x64 (static musl) and macOS 11+ on Apple silicon.

[Unreleased]: https://github.com/vsolano9/mcp-standby/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/vsolano9/mcp-standby/releases/tag/v1.0.0
