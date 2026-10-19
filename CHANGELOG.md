# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- README: how to run the macOS binary when a browser download is quarantined.
- README: checksum commands per platform, and `cargo install` pinned to the release tag.

### Fixed

- A background list refresh cut short because the session ended (or the server was replaced) now logs at debug level instead of `warn background list refresh failed` ([#1](https://github.com/vsolano9/mcp-standby/issues/1)). A refresh against a server that died on its own still warns.

## [1.0.0] - 2026-10-16

First public release.

### Added

- `serve`: a stdio MCP proxy that answers `initialize`, `ping`, the list methods and `server/discover` from a cached manifest and starts the real server on the first request that needs it, queueing messages during the start.
- `refresh`: records a server's `initialize` result and every list (following pagination cursors) into a manifest.
- Live lists once the server runs; background comparison with the manifest after a deferred start, with an atomic rewrite and `list_changed` to the client when they differ; manifest update when the server sends `list_changed` itself (one refresh at a time, after a wait that doubles from 1 s up to 60 s while announcements keep coming).
- Server-initiated requests and notifications are held until the client has its `initialize` answer and has sent `notifications/initialized`; client notifications sent during a start are queued. Server-initiated requests reach the client under proxy ids, so a late answer never reaches a restarted server, and a server cancellation for a request already answered is dropped; server pings are answered by the proxy.
- Cached `initialize` answers only with a protocol version the server is known to agree on for the version the client asks; otherwise the server is started once to negotiate and the answer is remembered, until the server or its command changes.
- Restart on the next request after a server crash or a failed start, with `-32603` errors for affected requests; a crash is noticed from the process itself, even when a child it left behind keeps its output open.
- Process-tree shutdown on stdin close: `taskkill /T /F` on Windows, SIGTERM then SIGKILL to the server's process group on Unix, also on SIGTERM, SIGINT and SIGHUP to the proxy.
- Command resolution through the absolute entries of `PATH` (and `PATHEXT` on Windows) without searching the current directory; `.cmd`/`.bat` launchers run through `cmd.exe` with arguments escaped for both of its parsing passes.
- Logs to stderr and a rotated file; launch arguments are not logged.
- Release builds for Windows x64, Linux x64 (static musl) and macOS 11+ on Apple silicon.

[Unreleased]: https://github.com/vsolano9/mcp-standby/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/vsolano9/mcp-standby/releases/tag/v1.0.0
