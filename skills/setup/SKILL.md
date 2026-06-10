---
name: setup
description: >
  Setup the Python environment for the orchestra plugin. Creates a virtual environment
  and installs the Python dependencies (from requirements.txt via pip) needed for each phase.
  Run this once before any other orchestra skill, or whenever dependencies are missing.
triggers:
  - "setup orchestra"
  - "bootstrap orchestra"
  - "install orchestra dependencies"
  - "orchestra environment"
  - "create orchestra venv"
  - "ModuleNotFoundError orchestra"
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

## Output

| Artifact | Description |
|---|---|
| venv (`<plugin_dir>/.venv` or `/Workspace/Users/<user>/.migration-skills`) | Virtual environment containing the installed dependencies |
| `<plugin_dir>/.migration-venv` | Marker file holding the resolved interpreter path |
| `requirements.txt` | The dependency list installed into the venv |

## Examples

- "Set up the orchestra environment"
- "Bootstrap orchestra so I can run a migration"
- "I got a ModuleNotFoundError running profile — fix the environment"
