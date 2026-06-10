---
name: translate
description: >
  Translate parsed ADF pipeline AST into Databricks IR (intermediate representation).
  Runs deterministic translators for known activity types, then invokes agentic skills
  from adf-to-databricks-plugin for gaps.
triggers:
  - "translate ADF"
  - "convert ADF"
  - "translate pipelines"
  - "convert pipelines"
  - "run translation"
---

# Translate ADF to Databricks IR

Convert the parsed ADF inventory into Databricks intermediate representation (IR) using deterministic translators for known types and agentic fallback for unknown types.

## Context

This is phase 2 of the orchestra migration workflow. It consumes the `inventory.json` produced by the `profile` skill and produces a `translation_report.json` that the `prepare` skill uses to generate Databricks Declarative Automation Bundles.

The translation follows a **deterministic-first** strategy:
1. Activities with known, well-defined mappings are translated by built-in Python translators
2. Activities that require interpretation, expression conversion, or lack a Python translator are handled by agent skills from the `adf-to-databricks-plugin`

## Prerequisite — Python environment

This skill runs the plugin's Python code, which depends on third-party packages. Before running
any Python commands, ensure the plugin's virtual environment is bootstrapped. Run the
**`setup`** skill, or directly:

```bash
bash <plugin_dir>/scripts/bootstrap.sh
```

This creates `<plugin_dir>/.venv` and installs dependencies from `requirements.txt`. If Python or 
pip is missing, the script prints a warning telling the user what to install — relay it and stop
until they have installed Python 3.12+ and pip.

Run **every** Python command in this skill with the venv interpreter and `src/` on `PYTHONPATH` 
(use it anywhere a command below shows `python3`):

```bash
export PYTHONPATH="<plugin_dir>/src"
"<plugin_dir>/.venv/bin/python" <plugin_dir>/src/orchestra/parser/adf_loader.py ...
```

## Workflow

Follow these steps in order:

### Step 0 — Gather phase inputs

Run the adapter inputs subcommand so the agent surfaces the free-text
options the phase needs (inventory path, ADF source dir, output
directory):

```bash
python3 -m orchestra.adapter inputs translate
```

The JSON response carries the prompts and defaults; collect answers
from the user (or fall back to the defaults) and persist them to
`<output_dir>/translate/inputs.json` so later steps and subsequent
phases can read the same values.

### Step 1 — Locate the inventory

Read `inventory.json` from the profile phase. If the path is not already in conversation context, ask the user:

> Where is the inventory.json from the profile phase? (default: `./orchestra_output/profile/inventory.json`)

Validate the file exists and is well-formed.

### Step 2 — Run deterministic translation

Execute the translation engine on all deterministic activities:

```bash
# Unified runner (recommended): `python -m orchestra.adapter translate ...`
# forwards to the engine below; --adf-source-path aliases --source-dir.
python3 <plugin_dir>/src/orchestra/translator/engine.py \
  --source-dir <adf_source_dir> \
  --output-dir <output_dir> \
  [--pipeline <pipeline_name>]
```

Where:
- `<plugin_dir>` is the root of the orchestra plugin
- `<adf_source_dir>` is the original ADF JSON directory (from the profile phase)
- `<output_dir>` is the translation output path (default: `./orchestra_output/translate/`)
- `<pipeline_name>` (optional) — when provided, translates only the named pipeline. **Always pass `--pipeline` when the user has specified a specific pipeline to migrate**, matching the value passed to the profile phase.

This produces:
- `<pipeline_name>.json` — the pipeline IR (one file per translated pipeline)
- `ir/` directory — Databricks IR for each translated activity
- `notebooks/` directory — generated helper notebooks

### Step 3 — Read the translation report

Read `translation_report.json`. It has this structure:

```json
{
  "inventory_path": "/path/to/inventory.json",
  "generated_at": "2026-04-07T12:30:00Z",
  "translations": [
    {
      "pipeline": "ETL_Main",
      "activity": "CopyFromBlob",
      "type": "Copy",
      "strategy": "deterministic",
      "status": "translated",
      "ir": {
        "task_key": "copy_from_blob",
        "task_type": "notebook_task",
        "notebook_path": "notebooks/copy_from_blob.py",
        "parameters": { "source": "abfss://...", "target": "..." }
      }
    },
    {
      "pipeline": "ETL_Main",
      "activity": "TransformData",
      "type": "ExecuteDataFlow",
      "strategy": "agentic",
      "status": "pending",
      "raw_activity_json": { "...": "..." },
      "target_skill": "adf-to-databricks:adf-dataflow-converter"
    }
  ],
  "summary": {
    "total": 47,
    "deterministic_translated": 35,
    "agentic_pending": 10,
    "failed": 2
  }
}
```

### Step 4 — Handle agentic gaps

For each translation with `"status": "pending"` and `"strategy": "agentic"`, invoke the appropriate skill from the `adf-to-databricks-plugin`. Route by activity type.

Every agentic gap in the translation report carries the activity's **full ADF/ARM JSON** under `raw_activity_json` (engine field `raw_definition`), and the generated placeholder notebook embeds the same JSON in a fenced `json` block. This holds for nested activities too — an `Until` inside an `IfCondition` / `Switch` / `ForEach` is reported as its own gap. Always translate from this ARM JSON.

**Until activities (agent-based handler):**
Databricks Lakeflow Jobs have no native repeat-until loop, so translate the `Until` from its ARM JSON into a single Python notebook task implementing a bounded polling loop. From the embedded JSON, read:
- `typeProperties.expression` — the ADF exit condition (e.g. `@or(equals(variables('jobStatus'),'succeeded'), equals(variables('jobStatus'),'failed'))`); convert it into the Python `while not (<condition>):` guard.
- `typeProperties.timeout` — wrap the loop in a wall-clock deadline (`time.monotonic()`), raising on timeout.
- `typeProperties.activities` — the loop body (e.g. a `Wait`, a polling `WebActivity`, a `SetVariable` that captures the next status); translate each child inline so the whole loop runs in one notebook.
Read the loop variables from `dbutils.widgets`, surface the final state as a task value, and write the result over the placeholder notebook's `raise NotImplementedError` cell. If the external `adf-to-databricks:adf-pipeline-converter` skill is installed you may delegate to it with the same ARM JSON; otherwise perform the translation directly.

**ExecuteDataFlow activities:**
Invoke `adf-to-databricks:adf-dataflow-converter` with the raw activity JSON and associated data flow definition. Provide context:
- The raw `typeProperties` from the ADF activity
- The data flow JSON definition (if available in the source directory under `dataflow/`)
- The linked service configurations for source/sink connections
- Target catalog and schema for the SDP pipeline or PySpark notebook output

**Control flow activities (Switch, Until, Wait, Filter, AppendVariable):**
Invoke `adf-to-databricks:adf-pipeline-converter` with the raw activity JSON. Provide context:
- The full pipeline JSON containing the activity
- Any nested activities within the control flow
- Variable definitions from the pipeline
- The desired Databricks task type mapping

**Stored procedures and external calls (SqlServerStoredProcedure, AzureFunction, WebHook, Custom):**
Invoke `adf-to-databricks:adf-pipeline-converter` with the raw activity JSON. Provide context:
- The linked service configuration for the target system
- Connection details and authentication method
- Any parameters or request bodies

**Complex expressions:**
If any activity (deterministic or agentic) contains ADF expressions that the deterministic translator could not resolve, invoke `adf-to-databricks:adf-expression-translator` with:
- The raw expression string (e.g., `@pipeline().parameters.inputPath`)
- The expression context (pipeline parameters, variables, activity outputs)
- The target format (Python f-string, Spark SQL, task parameter reference)

**Trigger definitions:**
Invoke `adf-to-databricks:adf-trigger-converter` with:
- The trigger JSON definition
- The associated pipeline references
- Target: Databricks job schedule configuration (quartz_cron_expression, periodic, or file_arrival)

### Step 5 — Collect agentic results

Each resolved agentic gap produces one translation result. Write them into
`<output_dir>/agentic_results/` as one JSON file per activity (the filename is
arbitrary, e.g. `<pipeline>__<activity>.json`). Each file MUST use this schema:

```json
{
  "activity_name": "<the placeholder activity name, exactly as in the report>",
  "pipeline": "<pipeline name>",
  "task": {
    "type": "NotebookActivity",
    "name": "<activity name>",
    "task_key": "<task key>",
    "notebook_path": "/Workspace/.../your_translated_notebook"
  }
}
```

- `activity_name` (required) — matches the `name` of the placeholder task in the
  report (the merge locates it by name, recursing into IfCondition / ForEach /
  Switch containers, so nested gaps like an `Until` are found).
- `pipeline` (optional) — only needed to disambiguate multi-pipeline reports.
- `task` (required) — the replacement IR task. The most portable form is a
  `NotebookActivity` whose `notebook_path` points at a notebook you have written
  to the workspace; the prepare phase references it directly. `task_key` and
  `depends_on` are inherited from the placeholder when omitted, so dependency
  edges are preserved.

### Step 6 — Merge agentic results

Fold the results into the translation report (placeholders are replaced in place):

```bash
python3 <plugin_dir>/src/orchestra/translator/engine.py \
  --merge-agentic \
  --report <translation_report_path> \
  --agentic-results <agentic_results_dir>
```

Equivalently via the unified runner: `python -m orchestra.adapter translate --merge-agentic --report <report> --agentic-results <dir>`. Add `--output <path>` to write a copy instead of overwriting the report. The command exits non-zero if any result could not be matched to a placeholder.

This updates `translation_report.json` with the agentic results merged in, changing their status from `pending` to `translated` (or `failed` if the agentic skill could not produce a result).

### Step 6.1 — Gather just-in-time translation configuration

The adapter raises several configuration options plus a chained set for
metadata-driven motifs. Every time the user answers a option whose value 
gates further prompts, re-run `inspect --answers <answers.json>` to surface 
the next batch.

**Copy→Notify (`copy_and_notify`) motifs.** When a Copy activity is followed by
notification Web activities, the adapter raises `copy_notify_destination`:
`keep` (default) leaves the Web activities to translate directly — nothing is
collapsed. Any other value (`email`, `slack`, `teams`, `pagerduty`, `webhook`)
collapses the pattern: the Copy becomes the task and the notifications become
Databricks job-task `on_success`/`on_failure` notifications routed to that
destination (the ADF Web activity URL/body is not used). Once a non-`keep`
destination is chosen, the adapter chains **one follow-up per Databricks-SDK
field** of that destination, required fields first, so each is prompted
sequentially (re-run `inspect --answers` after each answer to surface the next):

| Destination | Chained field options (SDK arg) |
|-------------|---------------------------------|
| `email`     | `copy_notify_email_recipients` (`addresses`, comma-separated) |
| `slack`     | `copy_notify_slack_url` (`url`), `copy_notify_slack_channel_id` (`channel_id`, optional), `copy_notify_slack_oauth_token` (`oauth_token`, optional) |
| `teams`     | `copy_notify_teams_url` (`url`) |
| `pagerduty` | `copy_notify_pagerduty_integration_key` (`integration_key`) |
| `webhook`   | `copy_notify_webhook_url` (`url`), `copy_notify_webhook_username` (`username`, optional), `copy_notify_webhook_password` (`password`, optional) |

All destinations also take an optional `copy_notify_destination_name` and
`copy_notify_events` (both/on_failure/on_success). Optional fields left blank are
omitted so the SDK applies its defaults. For **non-email** destinations, the
`modify` phase creates (or reuses by display name) the Databricks notification
destination via the SDK **as soon as you submit the answers** — it validates the
config immediately and bakes the resolved destination id into the modified report,
so prepare just wires `webhook_notifications` to that id (no further SDK call).
This requires workspace auth at `modify` time; if creation fails there, the id is
left unresolved and prepare retries or emits a `notification_destination` setup task.
**Email** needs no destination — it uses raw `email_notifications` and is never
created via the SDK.

When the metadata-driven flow ends with `metadata_driven_lookup_tool=have`
and the agent has a database tool (Genie, MCP SQL, or a workspace SDK),
run the lookup query directly and write the rows to
`<output_dir>/lookup_values.json`. When the answer is `none`, prompt
the user for a CSV file or comma-separated string and call:

```bash
python3 -m orchestra.adapter materialize-lookup "<csv-or-path>" \
    --out <output_dir>/lookup_values.json
```

Then call `modify` with the lookup values:

```bash
python3 -m orchestra.adapter modify \
    <translation_report_path> \
    <output_dir>/answers.json \
    --lookup-values <output_dir>/lookup_values.json \
    --out <output_dir>/translation_report.stamped.json
```

When no metadata-driven motif is consolidated, `--lookup-values` is omitted.

#### Legacy flow details

Before writing the final report, surface any pipeline-modifier options the
IR raises (Copy Data paradigm, non-Databricks task compute, Lakeflow Connect
opt-in, Databricks task compute). Use the adapter CLI bridge:

```bash
python3 -m orchestra.adapter inspect <translation_report_path>
```

The command emits JSON:

```json
{
  "pipelines": [
    {
      "pipeline_name": "ETL_Main",
      "options": [
        {
          "option_id": "copy_activity_paradigm",
          "prompt": "How should Copy Data activities targeting Delta be implemented?",
          "rationale": "...",
          "options": [{"value": "notebook", "label": "...", "description": "..."}, ...],
          "affected_task_keys": ["copy_orders", "copy_customers"],
          "default": "notebook"
        },
        ...
      ]
    }
  ]
}
```

For each option, prompt the user with the rationale, options, and the
task keys it affects. Use the default when the user defers. Collect the
answers into a JSON file (`<output_dir>/answers.json`) shaped like:

```json
{
  "copy_activity_paradigm": "sdp",
  "non_databricks_task_compute": "serverless",
  "use_lakeflow_connectors": "lakeflow_connect"
}
```

Then apply the answers to produce a stamped report the prepare phase consumes:

```bash
python3 -m orchestra.adapter modify \
  <translation_report_path> \
  <output_dir>/answers.json \
  --out <output_dir>/translation_report.stamped.json
```

The prepare phase (next skill) must be pointed at the stamped report.
When no options are raised, the inspect output is `{"pipelines": [{"pipeline_name": "...", "options": []},...]}` — skip the modify step and pass the original report straight through.

### Step 7 — Present translation summary

Display a summary to the user:

```
Translation Summary
===================
Total activities:           47
Deterministic translated:   35 (74.5%)
Agentic translated:          8 (17.0%)
Failed:                      4 ( 8.5%)

Overall coverage:           91.5%

Failed translations:
  - ETL_Main / RunSSIS (ExecuteSSISPackage) — no translator available
  - ETL_Main / CustomTask (Custom) — agentic skill returned error
  ...

Generated artifacts:
  - translation_report.json
  - ir/ (43 files)
  - notebooks/ (12 files)
```

If coverage is below 100%, explain the options for failed translations:
1. Manual notebook creation for unsupported types
2. Retry agentic translation with additional context
3. Skip the activity and add a placeholder task in the DAB

## Reference

See `references/activity-mapping.md` for the complete mapping between ADF activity types and translation strategies.

## Examples

- "Translate the ADF pipelines"
- "Convert ADF to Databricks"
- "Run the translation on the inventory from the profile step"
- "Translate the parsed pipelines using deterministic + agentic"
- "Translate only the pl_demo_01 pipeline"

## Output Artifacts

| File | Description |
|---|---|
| `translation_report.json` | Full translation report with IR for all activities |
| `ir/*.json` | Databricks IR for each translated activity |
| `notebooks/*.py` | Generated helper notebooks |
| `agentic_results/*.json` | Raw results from agentic skill invocations |
