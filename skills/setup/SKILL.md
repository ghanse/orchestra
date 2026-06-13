---
name: setup
description: >
  Setup the Python environment for the orchestra plugin. Creates a virtual environment
  and installs the Python dependencies (from requirements.txt via pip) needed for each phase,
  then makes the phases available as MCP tools — installing the MCP server locally for
  Claude Code / other agents, or deploying it as a Databricks App for Genie Code.
  Run this once before any other orchestra skill, or whenever dependencies are missing.
triggers:
  - "setup orchestra"
  - "bootstrap orchestra"
  - "install orchestra dependencies"
  - "orchestra environment"
  - "create orchestra venv"
  - "ModuleNotFoundError orchestra"
  - "install orchestra mcp"
  - "deploy orchestra mcp"
---

# Create the Python Environment

Create a virtual environment for the plugin and install its Python dependencies. This
is the prerequisite for the `profile`, `translate`, `prepare`, and `migrate` skills which run Python
from this environment.

The venv location depends on where you run:
- **Databricks (Genie Code / notebooks):** `/Workspace/Users/<current user>/.migration-skills` — it
  persists in the workspace and is reused across sessions.
- **Everywhere else:** `<plugin_dir>/.venv`.

In both cases the bootstrap script writes the resolved interpreter path to
`<plugin_dir>/.migration-venv`. **Every other skill reads that marker file to find the interpreter**,
so you never hard-code the venv location.

## Context

The orchestra plugin ships Python code (in `src/orchestra/`) that the skills invoke (e.g. 
`python -m orchestra.adapter ...`, `adf_loader.py`, `engine.py`, `dab_writer.py`). Some code depends 
on third-party packages (`pyyaml`, `databricks-sdk`, `sqlglot`). Running it against a bare system 
Python fails with `ModuleNotFoundError`. This step provisions an isolated venv with the required
dependencies installed via `pip` from `requirements.txt`.

The environment is created once and reused. Re-running the bootstrap script confirms the venv exists 
and ensures that dependencies are satisfied.

## Workflow

### Step 1 — Run the bootstrap script

From the plugin root, run:

```bash
bash <plugin_dir>/scripts/bootstrap.sh
```

Where `<plugin_dir>` is the root of the orchestra plugin (the directory containing `src/`,
`skills/`, and `requirements.txt`).

The script will:
1. Check that `python3`, `pip`, and the `venv` module are available.
2. Resolve the venv location: `/Workspace/Users/<current user>/.migration-skills` on Databricks
   (current user read from the notebook runtime context), otherwise `<plugin_dir>/.venv`.
3. Create the venv if it does not already exist.
4. Install dependencies listed in `requirements.txt` into that venv using `pip`.
5. Write the resolved interpreter path to `<plugin_dir>/.migration-venv` for the other skills.

### Step 1b — Databricks serverless / cluster environments

On Databricks serverless compute (Genie Code, notebook serverless, etc.), `ensurepip` is not
bundled with the system Python. The bootstrap script **automatically handles this** by falling
back to `python3 -m venv --without-pip` and then bootstrapping pip via `get-pip.py` from
`https://bootstrap.pypa.io`. No manual intervention is required.

Authentication for workspace downloads is also auto-configured: the runtime's notebook context
provides host + token, which is written to `~/.databrickscfg` on first use.

> **Note:** `databricks bundle validate` and other interactive CLI commands are **not available**
> on serverless compute. Use the web terminal or a local machine for bundle deployment steps.

### Step 2 — Handle a missing Python or pip

If Python, pip, or the `venv` module are **not** available, the script prints a `WARNING:` block
explaining what to install and exits non-zero **without** creating anything.

When this happens, **do not attempt to work around it**. Relay the warning to the user, ask them
to install, and stop:

> ⚠️ Python must be installed before I can set up the orchestra environment.
> 
>  * On macOS: `brew install python`.
>  * On Debian/Ubuntu: `sudo apt-get install python3 python3-venv python3-pip`.
> 
> Let me know once it's installed and I'll re-run setup.

Re-run this setup skill after the user confirms Python and pip are installed.

### Step 3 — Confirm success and how to run Python code

On success, the script prints the interpreter path (and writes it to `<plugin_dir>/.migration-venv`).
After this, every Python command in the orchestra skills **must** be run with the venv interpreter
and `src/` on `PYTHONPATH`. Resolve the interpreter from the marker file:

```bash
export PYTHONPATH="<plugin_dir>/src"
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m orchestra.adapter inputs profile
```

`$PY` resolves to `/Workspace/Users/<current user>/.migration-skills/bin/python` on Databricks or
`<plugin_dir>/.venv/bin/python` locally. Use `"$PY"` anywhere the other skills show `python3` or the
interpreter. (On Windows the local interpreter is `<plugin_dir>\.venv\Scripts\python.exe`.)

### Step 4 — Make the orchestra phases available as MCP tools

The phases are also packaged as MCP tools (see `src/orchestra/mcp/`) so an agent can invoke
them directly instead of shelling out to the CLI. **How they are exposed depends on the
environment** — branch on the same `DATABRICKS_RUNTIME_VERSION` signal `bootstrap.sh` uses:

```bash
PY="$(cat <plugin_dir>/.migration-venv)"
if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]; then
  # Databricks Genie Code → host the tools as a Databricks App (Genie connects to its URL)
  bash <plugin_dir>/app/deploy.sh
else
  # Claude Code / other local agents → install the MCP server for local (stdio) hosting
  "$PY" -m pip install "mcp>=1.12" "uvicorn>=0.30" "starlette>=0.40"
fi
```

#### Local / Claude Code / other agents (`DATABRICKS_RUNTIME_VERSION` unset)

The `pip install` above adds the MCP server stack to the venv. Run the server over stdio and
register it with the MCP client. Resolve `command` from the marker file:

```bash
PYTHONPATH="<plugin_dir>/src" "$PY" -m orchestra.mcp        # stdio (default)
```

```json
{
  "mcpServers": {
    "orchestra": {
      "command": "<interpreter from .migration-venv>",
      "args": ["-m", "orchestra.mcp"],
      "env": { "PYTHONPATH": "<plugin_dir>/src" }
    }
  }
}
```

#### Databricks Genie Code (`DATABRICKS_RUNTIME_VERSION` set)

`app/deploy.sh` stages a self-contained bundle (the app entrypoint plus a vendored copy of the
orchestra source), syncs it to **`/Workspace/Shared/mcp-orchestra`**, and creates/deploys the
**`mcp-orchestra`** Databricks App. The script prints the app URL; the MCP endpoint is
`<app-url>/mcp`. (The `mcp-` prefix makes it auto-listed in the AI Playground; Genie Code's
**Custom MCP server** picker also selects any app by name.)

> **Clone into a shared location.** The app's service principal cannot read private
> `/Workspace/Users/<you>` folders by default, so `deploy.sh` deploys the source from
> `/Workspace/Shared/<app-name>`. For this to work, clone orchestra into a Git folder under
> **`/Workspace/Shared`** (e.g. `/Workspace/Shared/orchestra`), not your user home. If the
> workspace restricts `/Workspace/Shared`, use another all-users location and pass it via
> `APP_SOURCE_PATH`.

After it deploys, relay these follow-up steps to the user (the script also prints them):

1. **App access:** grant **Can use** on `mcp-orchestra` to the users / service principals that will
   call it (Apps UI → *Permissions*, or `databricks apps set-permissions mcp-orchestra ...`).
2. **Data access:** grant the app's service principal access to the catalogs, schemas, and volumes
   the migration touches (plus any SQL warehouse used by the reporting tools).
3. **Add it in Genie Code (Agent mode):** open Genie Code **Settings → MCP Servers → Add Server**,
   choose **Custom MCP server**, select the `mcp-orchestra` app, and **Save**. The `orchestra_*`
   tools then appear (MCP needs Agent mode; access is capped at 20 tools across all servers).
   The orchestra server is already **stateless** (`stateless_http=True`) with CORS, as Genie Code
   requires. If a browser CORS error appears, set the app env var `ORCHESTRA_ALLOWED_ORIGINS` to the
   workspace URL and redeploy. Verify via the health endpoint `<app-url>/`.

> **Note:** `databricks apps` deploy commands require a Databricks CLI session (workspace web
> terminal or a local machine), not serverless notebook Python. If the Genie session can't shell
> out to the CLI, run `app/deploy.sh` from the web terminal. (Same constraint as `databricks
> bundle deploy`.) If you see `Error: please specify target`, the CLI attached to a stray
> `databricks.yml`; `deploy.sh` already isolates against this, so re-run it as-is.

## Output

| Artifact | Description |
|---|---|
| venv (`<plugin_dir>/.venv` or `/Workspace/Users/<user>/.migration-skills`) | Virtual environment containing the installed dependencies |
| `<plugin_dir>/.migration-venv` | Marker file holding the resolved interpreter path |
| `requirements.txt` | The dependency list installed into the venv |
| MCP server (local) | `mcp` / `uvicorn` / `starlette` installed into the venv; run with `python -m orchestra.mcp` |
| MCP server (Databricks Genie Code) | `mcp-orchestra` Databricks App serving the tools at `<app-url>/mcp` (add via Genie Code → Custom MCP server) |

## Examples

- "Set up the orchestra environment"
- "Bootstrap orchestra so I can run a migration"
- "I got a ModuleNotFoundError running profile — fix the environment"
- "Install the orchestra MCP server" (local) / "Deploy the orchestra MCP server to Databricks" (Genie Code)
