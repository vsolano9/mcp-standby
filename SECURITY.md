# Security policy

## Scope

`mcp-standby` is a local stdio proxy between an MCP client and one MCP server that the client's own configuration names. It has no network listener, telemetry, update checker or network requests of its own. The server it starts runs with the user's permissions, exactly as it would without the proxy.

It is not a sandbox, permission layer or policy gate. Requests, responses and server-initiated messages (sampling, roots, elicitation) are forwarded unchanged. Use your client's own approval settings for tool calls.

## Local data

- **Manifests** (`<data dir>/manifests/<name>.json`) record the server's `initialize` result, its tool, prompt and resource lists, and its launch command and arguments. They never record environment variables. A secret passed as a command-line argument would therefore be written to the manifest: pass secrets through environment variables (the client's `env` block or `--env`) instead.
- **Logs** (`<data dir>/logs/<name>.log`, 1 MB with 3 rotations, or none with `--no-log-file`) record lifecycle events, the server's stderr, and the command name with its argument count, not the arguments. A server that prints secrets to stderr puts them in the log; the proxy cannot tell.
- The data directory is `MCP_STANDBY_HOME`, else `%LOCALAPPDATA%\mcp-standby` on Windows, else `$XDG_DATA_HOME/mcp-standby` or `~/.local/share/mcp-standby`. Keep it private to your user. A manifest someone else can write could change the tools your client sees until the server starts and the proxy refreshes the lists.

Commands are resolved through `PATH` (and `PATHEXT` on Windows) without searching the current directory, so a file planted in a repository the client opens cannot stand in for the server. An untrusted directory on `PATH` is still part of your trust boundary.

## Report a vulnerability

Report sensitive findings privately through [GitHub's private vulnerability reporting form](https://github.com/vsolano9/mcp-standby/security/advisories/new). Include the version or commit, OS, client, reproduction steps, expected and observed behavior, and impact. Remove credentials and private data from the report.

Use public issues for ordinary bugs. No response-time or remediation deadline is promised. Fixes target the latest release.
