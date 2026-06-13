---
name: migrate
description: >
  End-to-end migration of Azure Data Factory pipelines to Databricks Lakeflow Jobs.
  Orchestrates profile, translate, and prepare phases in sequence.
triggers:
  - "migrate ADF"
  - "migrate pipelines"
  - "ADF to Databricks"
  - "migrate to Lakeflow"
  - "ADF migration"
  - "convert ADF to Lakeflow"
  - "migrate data factory"
---

# End-to-End ADF to Databricks Migration

Orchestrate the complete migration of Azure Data Factory pipelines to Databricks Lakeflow Jobs via Declarative Automation Bundles. This skill runs all three phases in sequence: profile, translate, prepare.

## Context

This is the top-level orchestration skill. It runs the full migration pipeline:

1. **Profile** — Parse ADF JSON exports into a typed inventory
2. **Translate** — Convert ADF activities to Databricks IR (deterministic + agentic)
3. **Prepare** — Generate Databricks Declarative Automation Bundles for deployment

Each phase builds on the output of the previous phase. The user is shown a summary and asked to confirm before proceeding to the next phase.

## How to run this skill — MCP tools or venv CLI

This skill orchestrates all three phases. Run the **`setup`** skill first if you haven't. There are
two execution paths:

### MCP tools (Databricks Genie Code, or a local stdio registration)

In Genie Code this is the **only** path — the phases run on the deployed `mcp-orchestra` app, so
there is **no venv, no `bootstrap.sh`, and no `.migration-venv`**. Run **no** `python3`/`$PY`/`bash`
commands on this path; the `"$PY" -m …` snippets in the steps below are the **local-CLI fallback
only**. Everything goes through the single **`orchestra`** tool, one `command` per step:

```
orchestra(command="inputs", parameters={"phase": "profile" | "translate" | "prepare"})  # learn each phase's inputs
orchestra(command="profile", parameters={"adf_source_path": ..., "output_dir": ..., "pipeline": ...})
orchestra(command="translate", parameters={"output_dir": ..., "adf_source_path": ..., "pipeline": ...})
orchestra(command="merge_agentic", parameters={"report_path": ..., "agentic_results_dir": ..., "output_path": ...})  # if agentic results
orchestra(command="inspect", parameters={"report_path": ...})
orchestra(command="apply_answers", parameters={"report_path": ..., "answers": [...], "output_dir": ...})
orchestra(command="prepare", parameters={"output_dir": ..., "catalog": ..., "schema": ...})
orchestra(command="record_results", parameters={...}) / orchestra(command="install_dashboard", parameters={...})
```

Or run the whole thing non-interactively with `orchestra(command="migrate", parameters={"adf_source_path": ...,
"output_dir": ..., "catalog": ..., "schema": ..., "pipeline": ...})` (uses default translation options — for
control over options, chain `command="inspect"` / `command="apply_answers"`). Each call returns a structured
result (summaries / file trees); use those in place of reading files. Wherever a step below shows
`"$PY" -m orchestra.adapter <cmd> …`, call `orchestra(command="<cmd>", parameters={...})` instead.

> `databricks bundle validate` / `deploy` of the *generated* bundle is still a user-driven CLI step
> (web terminal / local / CI-CD); present the bundle for review.

### venv CLI (local, no MCP server)

Ensure the venv exists (`setup` Path B / `bootstrap.sh`), then run the commands below with the venv
interpreter (from the marker file `<plugin_dir>/.migration-venv`) and `src/` on `PYTHONPATH` (use
`$PY` anywhere a command shows `python3`):

```bash
export PYTHONPATH="<plugin_dir>/src"
PY="$(cat <plugin_dir>/.migration-venv)"
"$PY" -m orchestra.adapter inputs profile
```

If Python or pip is missing, `bootstrap.sh` prints a warning telling the user what to install — relay
it and stop until they have Python 3.12+ and pip.

## Workflow

Follow these steps in order:

### Step 0 — Gather phase inputs via the adapter

Before invoking profile, run the adapter inputs subcommand once per
phase so the agent surfaces the matching free-text prompts:

```bash
"$PY" -m orchestra.adapter inputs profile
"$PY" -m orchestra.adapter inputs translate
"$PY" -m orchestra.adapter inputs prepare
```

Each response carries the options for that phase plus their descriptions and
defaults.  Collect answers from the user (or accept the defaults) and thread the
values into the downstream CLI calls. All phases share **one** migration
`<output_dir>` (default `./orchestra_output`).

### Step 1 — Gather inputs

Ask the user for all required inputs upfront:

| Parameter | Description | Required | Default |
|---|---|---|---|
| ADF source path | UC volume path or local directory with ADF JSON files | Yes | — |
| Output directory | Single shared root for all orchestra output (bundle + `metadata/`) | No | `./orchestra_output` |
| Target catalog | Unity Catalog catalog for tables/volumes | No | `main` |
| Target schema | Schema within the catalog | No | `default` |
| Bundle name | Name for the generated DABs project | No | derived from pipelines |

Example prompt:

> To migrate your ADF pipelines, I need:
> 1. Where are your ADF JSON exports? (UC volume path like `/Volumes/main/default/adf_export` or local directory)
> 2. Where should I write the output? (default: `./orchestra_output/`)
> 3. What target catalog and schema? (default: `main.default`)

### Step 2 — Phase 1: Profile

Invoke the `orchestra:profile` skill with the ADF source path and `--output-dir <output_dir>` (the shared migration dir). Profile writes `<output_dir>/metadata/{inventory.json, profile_report.csv, <pipeline>.arm.json}`.

Wait for the profile to complete and present the inventory summary:

```
Phase 1: Profile — Complete
==========================
Pipelines parsed:     12
Total activities:     47
  Deterministic:      35 (74.5%)
  Agentic:            10 (21.3%)
  Unsupported:         2 ( 4.3%)
Coverage:             95.7%
```

### Step 3 — Checkpoint: confirm proceed

Ask the user to review the inventory and confirm before continuing:

> The profile phase found 47 activities across 12 pipelines. 95.7% have a translation path (74.5% deterministic, 21.3% agentic). 2 activities are unsupported and will need manual handling.
>
> Proceed to the translation phase? (yes/no)

If the user says no, explain the options:
- Re-run profile with a different source directory
- Review `<output_dir>/metadata/inventory.json` (and `profile_report.csv`) to understand unsupported activities and pipeline complexity
- Manually classify activities before proceeding

If the user says yes, proceed to step 4.

### Step 4 — Phase 2: Translate

Invoke the `orchestra:translate` skill with:
- ADF source dir: the original ADF source path (same `--source-dir` as profile)
- Output dir: the same shared `<output_dir>` (translate writes its report to `<output_dir>/.work/`)

Wait for the translation to complete and present the summary:

```
Phase 2: Translate — Complete
=============================
Deterministic translated:   35 (74.5%)
Agentic translated:          8 (17.0%)
Failed:                      4 ( 8.5%)
Overall coverage:           91.5%
```

### Step 5 — Present translation details

Show the user:
1. What was translated deterministically (bulk — just counts by type)
2. What was translated via agentic skills (list each with the skill used)
3. What failed and why (list each with the failure reason)

For failures, suggest:
- Manual notebook creation
- Retry with additional context
- Skip and add placeholder

### Step 5.1 — Gather just-in-time translation configuration

Drive the loop multi-pass: re-run `inspect` with all answers collected so far
appended as `--answer OPTION_ID=VALUE` flags so the adapter can surface chained
metadata-driven prompts. When the user opts to consolidate a metadata-driven motif
and the agent has a database tool, run the lookup query to get CSV rows; otherwise
prompt the user for a CSV file path or literal CSV string. Pass it inline to
`modify` via `--lookup-csv "<csv-or-path>"` (no intermediate JSON file).

#### Legacy flow details

Before bundle generation, run the adapter inspect CLI on the translation
report to surface any pipeline-modifier options the IR raises:

```bash
"$PY" -m orchestra.adapter inspect <output_dir>/.work/translation_report.json
```

For each option in the JSON output, prompt the user with the rationale, options,
and the affected task keys. Then apply the collected answers as repeatable
`--answer OPTION_ID=VALUE` flags:

```bash
"$PY" -m orchestra.adapter modify \
  <output_dir>/.work/translation_report.json \
  --output-dir <output_dir> \
  --answer copy_activity_paradigm=sdp \
  --answer non_databricks_task_compute=serverless \
  [--lookup-csv "<csv-or-path>"]
```

`modify` writes the stamped report to `<output_dir>/.work/translation_report.stamped.json`
and the kept answers record to `<output_dir>/metadata/configuration.json`. The prepare phase
reads the stamped report from `.work/` automatically. When inspect emits no options for any
pipeline, skip `modify` — prepare falls back to the un-stamped report.

The options the adapter raises:

| `option_id` | Allowed values | Default |
|---|---|---|
| `copy_activity_paradigm` | `notebook`, `sdp` | `notebook` |
| `non_databricks_task_compute` | `serverless`, `classic` | `serverless` |
| `use_lakeflow_connectors` | `existing`, `lakeflow_connect` | `existing` |
| `consolidate_motif:<motif_id>` | `keep`, `consolidate` | `keep` |

DatabricksNotebook and DatabricksSparkPython tasks always inherit the cluster binding derived from
their source linked service.

For each multi-activity motif the detector matches (rest_api_pagination,
incremental_load_watermark, metadata_driven_bulk_copy, ...) the adapter emits one
`consolidate_motif:<motif_id>` option. The user must explicitly opt in to `consolidate` 
for each detected pattern.

### Step 6 — Checkpoint: confirm proceed to bundle generation

> Translation is 91.5% complete. 4 activities could not be translated automatically.
> Options:
> 1. Proceed to bundle generation (failed activities will get placeholder tasks)
> 2. Retry failed translations with more context
> 3. Stop here and review the translation report
>
> What would you like to do?

### Step 6.5 — Detect workspace artifacts and authenticate

Before invoking the prepare phase, run the adapter's
`workspace-paths` subcommand to detect any absolute workspace paths
the bundle would need to download:

```bash
"$PY" -m orchestra.adapter workspace-paths \
  <output_dir>/.work/translation_report.stamped.json \
  --source-dir <adf_source_path>
```

When the response carries `needs_auth: true`:

1. Confirm the workspace host with the user, defaulting to the first
   entry in `suggested_hosts` (extracted from the Databricks linked
   services in the ADF export).
2. Run `databricks auth login --host <host>` interactively to set up
   a local profile.
3. Pass `--profile <name>` to the prepare invocation in Step 7 so
   orchestra downloads the referenced notebooks and downloads them under
   `bundle/src/notebooks/` with the task references rewritten to the
   relative `../src/notebooks/...` paths.

Skip this step entirely when `needs_auth` is `false`.

### Step 7 — Phase 3: Prepare

Invoke the `orchestra:prepare` skill with:
- Output dir: the same shared `<output_dir>` — prepare reads the stamped report from
  `<output_dir>/.work/` automatically (no report path needed) and writes the bundle here
- Catalog: user-specified or `main`
- Schema: user-specified or `default`

Prepare prunes the transient `<output_dir>/.work/` after a successful build, leaving the
bundle (databricks.yml, resources/, src/, SETUP.md) plus the kept `metadata/` folder.

### Step 7.5 — (Optional) Persist coverage results and install a dashboard

When running with workspace auth (Genie Code or a configured profile), offer to record this
run's migration coverage to a Unity Catalog table and optionally install a coverage dashboard.
The `inputs prepare` prompts surface `results_table`, `results_warehouse_id`, and
`install_dashboard`.

If the user supplies a `results_table`:

```bash
"$PY" -m orchestra.adapter record-results \
  --output-dir <output_dir> --results-table <catalog.schema.table> [--warehouse-id <id>]
```

Writes one row per pipeline (counts, complexity size, deterministic/agentic/unsupported
coverage), each stamped with a UUID `run_id`, `run_date` (`CURRENT_TIMESTAMP()`), and `run_by`
(`CURRENT_USER()`). If `install_dashboard = yes`:

```bash
"$PY" -m orchestra.adapter install-dashboard --results-table <catalog.schema.table> [--warehouse-id <id>]
```

Creates and publishes an AI/BI coverage dashboard over the table and prints its URL. Both
auto-detect a SQL warehouse when `--warehouse-id` is omitted and degrade gracefully without
workspace auth. See the `prepare` skill (Step 8) for details.

### Step 8 — Present final summary

Display the complete migration summary:

```
Migration Complete
==================

Source:    /Volumes/main/default/adf_export (12 ADF pipelines)
Output:    ./orchestra_output/  (bundle + metadata/)

Coverage:
  Total activities:           47
  Successfully translated:    43 (91.5%)
  Placeholder tasks:           4 ( 8.5%)

Generated Files (under ./orchestra_output/):
  databricks.yml
  resources/          (3 job definitions)
  src/notebooks/      (12 notebooks)
  src/setup/          (3 setup scripts)
  SETUP.md
  metadata/           inventory.json, profile_report.csv, <pipeline>.arm.json, configuration.json

Setup Required:
  - Run setup/create_volumes.py to create UC volumes
  - Run setup/create_secrets.py to configure secrets (review credentials first)
  - Run setup/register_connections.py to register external connections

Next Steps:
  1. cd ./orchestra_output/
  2. Review generated files, especially notebooks and setup scripts
  3. databricks bundle validate --target dev
  4. Run setup scripts on the target workspace
  5. databricks bundle deploy --target dev
  6. databricks bundle run <job_name> --target dev
  7. Verify job output and promote to staging/prod
```

### Step 9 — Offer follow-up actions

Ask if the user wants to:
1. Validate the bundle now (`databricks bundle validate`)
2. Deploy to dev (`databricks bundle deploy --target dev`)
3. Review specific generated files
4. Re-translate any failed activities
5. Export a migration report for documentation

## Reference

See `references/workflow.md` for a detailed description of the three-phase architecture.

## Examples

- "Migrate my ADF pipelines to Databricks"
- "Convert ADF to Lakeflow jobs"
- "ADF to Databricks migration from /Volumes/main/default/adf_export"
- "Migrate data factory pipelines to catalog analytics, schema bronze"
- "Run the full ADF migration workflow"

## Output Artifacts

All three phases write into a single shared `<output_dir>` (default `./orchestra_output`):

| Path | Phase | Contents |
|---|---|---|
| `metadata/` | Profile + Modify | `inventory.json`, `profile_report.csv`, `<pipeline>.arm.json`, `configuration.json` |
| `databricks.yml`, `resources/`, `src/`, `SETUP.md` | Prepare | The deployable DAB bundle |
| `.work/` | Translate/Modify (transient) | Translation report + IR; pruned by prepare |
