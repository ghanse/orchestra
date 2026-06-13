# orchestra MCP server (Databricks App)

Hosts orchestra's migration phases and helper operations as [Model Context
Protocol](https://modelcontextprotocol.io) tools so agentic clients (Claude
Code, Claude Desktop, Databricks Genie Code) can drive an ADF → Databricks
Lakeflow migration by calling tools instead of shelling out to a CLI.

The MCP server itself lives in the orchestra package at
[`src/orchestra/mcp/`](../src/orchestra/mcp); this directory is just the
Databricks App wrapper and deployment tooling.

## Tools

| Tool | Wraps | Purpose |
|------|-------|---------|
| `orchestra_phase_inputs` | `adapter inputs` | List a phase's input prompts/defaults |
| `orchestra_profile` | `adapter profile` | Parse ADF JSON, classify activities |
| `orchestra_translate` | `adapter translate` | ADF activities → Databricks IR |
| `orchestra_inspect_report` | `adapter inspect` | Surface pending translation options |
| `orchestra_apply_answers` | `adapter modify` | Apply answers → stamped IR |
| `orchestra_materialize_lookup` | `adapter materialize-lookup` | CSV → lookup-values JSON |
| `orchestra_workspace_paths` | `adapter workspace-paths` | Detect workspace paths / hosts |
| `orchestra_prepare` | `adapter prepare` | Emit the deployable DAB bundle |
| `orchestra_migrate` | profile→translate→prepare | Full non-interactive migration |
| `orchestra_record_results` | `adapter record-results` | Write coverage to a UC table |
| `orchestra_install_dashboard` | `adapter install-dashboard` | Publish the coverage dashboard |

Each tool is a thin bridge over `python -m orchestra.adapter` (the same entry
point the agent skills use), then reads back the JSON/CSV artifacts each phase
writes — so the MCP surface stays in lockstep with the tested CLI contract.

## Run locally

```bash
pip install -e ".[mcp]"            # from the repo root

# stdio transport (Claude Code / Claude Desktop):
python -m orchestra.mcp

# streamable-HTTP transport (same server Databricks Apps runs):
python -m orchestra.mcp --http --port 8000
# MCP endpoint: http://localhost:8000/mcp   health: http://localhost:8000/
```

Register the stdio server with a local MCP client, e.g.:

```json
{ "mcpServers": { "orchestra": { "command": "python", "args": ["-m", "orchestra.mcp"] } } }
```

## Deploy as a Databricks App (for Genie Code)

```bash
# Authenticated Databricks CLI (v0.230+) required.
./app/deploy.sh
```

`deploy.sh` stages a self-contained bundle in a temporary directory outside the repo (the app entrypoint
plus a vendored copy of the pure-Python `orchestra` package), syncs it to your
workspace, and creates/deploys the app (default name **`mcp-orchestra`**).

> **Clone into `/Workspace/Shared`.** `deploy.sh` deploys the app source from
> `/Workspace/Shared/<app-name>` because the app's service principal **cannot read
> private `/Workspace/Users/<you>` folders** by default. Clone orchestra into a Git
> folder under `/Workspace/Shared` (e.g. `/Workspace/Shared/orchestra`); if that
> folder is restricted in your workspace, use another all-users location and pass it
> via `APP_SOURCE_PATH`.

End-to-end, to use it from Genie Code:

1. **Deploy** with `./app/deploy.sh`. The app is named `mcp-orchestra` and deploys the
   source from `/Workspace/Shared/mcp-orchestra` (override with `APP_SOURCE_PATH`). The
   script prints the app URL; the MCP endpoint is `<app-url>/mcp`.
2. **Grant app access:** give **Can use** on `mcp-orchestra` to the users / service
   principals that will call it (Apps UI → *Permissions*, or
   `databricks apps set-permissions mcp-orchestra ...`).
3. **Grant data access:** the app authenticates as its own service principal, so
   grant that principal access to the catalogs / schemas / Unity Catalog volumes
   the migration reads/writes (and any SQL warehouse used by `record-results` /
   `install-dashboard`).
4. **Add it in Genie Code (Agent mode):** open Genie Code **Settings → MCP Servers →
   Add Server**, choose **Custom MCP server**, select the `mcp-orchestra` app, and
   **Save**. The `orchestra_*` tools become available immediately. Verify via the
   health endpoint `<app-url>/`.

> The `mcp-` name prefix also makes the app **auto-listed in the AI Playground**. Genie Code's
> **Custom MCP server** picker selects any Databricks App by name regardless of prefix.

Genie Code requires a custom MCP app to be (1) in the **same workspace**, (2) reachable
at `https://<app-url>/mcp`, and (3) **stateless** — this server sets
`stateless_http=True` and adds CORS, so it qualifies. If a browser CORS error appears,
set the app env var `ORCHESTRA_ALLOWED_ORIGINS` to your workspace URL and redeploy.
MCP access is capped at **20 tools** across all servers (orchestra exposes 11).

> Run `./app/deploy.sh` from a Databricks CLI session (workspace web terminal or a
> local machine) — `databricks apps` deploy is not available from serverless
> notebook Python. See [Connect Genie Code to MCP servers](https://learn.microsoft.com/en-us/azure/databricks/genie-code/mcp)
> and [host a custom MCP server](https://docs.databricks.com/aws/en/generative-ai/mcp/custom-mcp).

## Troubleshooting

**Genie Code can't connect / add the server (is it CORS or the server?)** — Tell them apart
from the app's logs (`databricks apps logs <app>` or the app UI):

- **Server-side error (not CORS):** the logs show a `500` / `RuntimeError: Task group is not
  initialized` on `POST /mcp`. That means the StreamableHTTP session manager never started —
  it happens if the MCP app is *mounted inside another Starlette app* (whose lifespan doesn't
  run the sub-app). The server now avoids this by serving FastMCP's own app directly; make sure
  you redeployed the current `app/`. Sanity-check the app is up with `curl <app-url>/`
  (expect `{"status":"ok"}`).
- **CORS:** the request reaches the server fine but the **browser console** shows a CORS error
  (blocked by `Access-Control-Allow-Origin`), with no corresponding 500 in the app logs. Set the
  app env var `ORCHESTRA_ALLOWED_ORIGINS` to your workspace URL and redeploy.



**`mkdir: cannot create directory ...: Permission denied`** — On Databricks (Genie web
terminal / serverless), `/tmp` and `$TMPDIR` are often not writable, while the `/Workspace`
filesystem where the repo lives is. `deploy.sh` stages the bundle in the **repo's parent
directory** first (e.g. `/Workspace/Shared`, which is writable and outside the git repo),
falling back to `$TMPDIR` / `/tmp` / `$HOME` for local runs. If it still can't find a writable
base, set `TMPDIR` to a writable path and re-run. (Staging is never placed inside the repo,
so `databricks sync` won't drop files via the repo's `.gitignore`.)

**`Error: please specify target`** — The Databricks CLI (v0.298+) makes `sync` and
`apps deploy` bundle-aware: if a `databricks.yml` is discoverable in the working
directory or any parent (for example a generated `orchestra_output/databricks.yml`,
or one in your workspace home), the CLI loads that bundle and—when it has multiple
targets with no default—aborts with this error before deploying. `deploy.sh` already
runs every CLI call from a throwaway directory to avoid this; if you invoke the CLI
manually, do the same (or pass `--target <name>`), and don't run it from inside a
generated bundle directory.

## Known constraints / follow-ups

- **Source/output locations.** Tools pass `adf_source_path` and `output_dir`
  straight to orchestra, which reads/writes via local filesystem paths. In a
  Databricks App, only the container's ephemeral disk is local; Unity Catalog
  Volume paths (`/Volumes/...`) are **not** auto-mounted. For hosted use, stage
  ADF exports onto a path the app can read (or extend `orchestra.mcp.runner` to
  fetch volume inputs via the SDK Files API into a temp dir before each phase).
  Locally hosted, ordinary paths and mounted volumes work as-is.
- **`databricks bundle validate/deploy`** of the *generated* DAB is a separate,
  user-driven step (run from a CLI session); it is intentionally not invoked by
  these tools.
- **Long-running phases.** Large factories can exceed default client timeouts;
  the server-side subprocess timeout is configurable via `ORCHESTRA_MCP_TIMEOUT`
  (seconds).
