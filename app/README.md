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

## Deploy as a Databricks App

```bash
# Authenticated Databricks CLI (v0.230+) required.
APP_NAME=orchestra-mcp ./app/deploy.sh
```

`deploy.sh` stages a self-contained bundle in `app/.build/` (the app entrypoint
plus a vendored copy of the pure-Python `orchestra` package), syncs it to your
workspace, and creates/deploys the app. The MCP endpoint is `<app-url>/mcp`.

Point Genie Code (or any MCP client that supports Databricks OAuth) at that
endpoint. The app authenticates to the workspace as its own service principal,
so grant that principal access to the catalogs/schemas/volumes the migration
needs (and to any SQL warehouse used by `record-results` / `install-dashboard`).

## Troubleshooting

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
