# ADF Activity Type to Databricks Translation Mapping

This reference defines the mapping between Azure Data Factory activity types and their translation strategy in orchestra.

## Strategy Definitions

- **Deterministic** — Handled by a built-in Python translator module. Fast, reliable, no LLM required. These mappings are well-defined and produce consistent output.
- **Agentic** — Handled by an LLM-assisted skill from the `adf-to-databricks-plugin`. Required when the ADF activity has complex semantics, requires interpretation, or lacks a direct Databricks equivalent.
- **Unsupported** — No automated translation path. Requires manual intervention.

## Activity Mapping Table

| ADF Activity Type | Strategy | Translator / Skill | Databricks Target |
|---|---|---|---|
| Copy | Deterministic | `copy.py` | `notebook_task` (Auto Loader / COPY INTO / JDBC) or `pipeline_task` (DLT) |
| DatabricksNotebook | Deterministic | `notebook.py` | `notebook_task` |
| DatabricksSparkJar | Deterministic | `spark_jar.py` | `spark_jar_task` |
| DatabricksSparkPython | Deterministic | `spark_python.py` | `spark_python_task` |
| ForEach | Deterministic | `for_each.py` | `for_each_task` |
| IfCondition | Deterministic | `if_condition.py` | `condition_task` |
| SetVariable | Deterministic | `set_variable.py` | `notebook_task` (task values) |
| Lookup | Deterministic | `lookup.py` | `notebook_task` |
| WebActivity | Deterministic | `web_activity.py` | `notebook_task` |
| Delete | Deterministic | `delete.py` | `notebook_task` (`dbutils.fs.rm`) |
| ExecutePipeline | Deterministic | `execute_pipeline.py` | `run_job_task` |
| DatabricksJob | Deterministic | `databricks_job.py` | `run_job_task` |
| ExecuteDataFlow | Agentic | `adf-to-databricks:adf-dataflow-converter` | DLT pipeline or PySpark notebook |
| Switch | Agentic | `adf-to-databricks:adf-pipeline-converter` | chained `condition_task`s |
| Until | Agentic | `adf-to-databricks:adf-pipeline-converter` | while-loop notebook |
| Wait | Agentic | `adf-to-databricks:adf-pipeline-converter` | `time.sleep` notebook |
| Filter | Agentic | `adf-to-databricks:adf-pipeline-converter` | filter notebook |
| AppendVariable | Agentic | `adf-to-databricks:adf-pipeline-converter` | task values notebook |
| SqlServerStoredProcedure | Agentic | `adf-to-databricks:adf-pipeline-converter` | SQL notebook |
| AzureFunction | Agentic | `adf-to-databricks:adf-pipeline-converter` | webhook/REST notebook |
| WebHook | Agentic | `adf-to-databricks:adf-pipeline-converter` | REST notebook |
| Custom | Agentic | `adf-to-databricks:adf-pipeline-converter` | custom notebook |
| ExecuteSSISPackage | Agentic | `adf-to-databricks:adf-pipeline-converter` | PySpark notebook |
| AzureMLExecutePipeline | Agentic | `adf-to-databricks:adf-pipeline-converter` | MLflow notebook |
| Triggers (Schedule) | Agentic | `adf-to-databricks:adf-trigger-converter` | `quartz_cron_expression` |
| Triggers (Tumbling Window) | Agentic | `adf-to-databricks:adf-trigger-converter` | periodic schedule |
| Triggers (Blob Event) | Agentic | `adf-to-databricks:adf-trigger-converter` | `file_arrival` trigger |

## Deterministic Translator Details

### Copy (`copy.py`)

Translates ADF Copy activities based on source/sink types:
- **Blob/ADLS to Delta** — Auto Loader (`cloudFiles`) notebook or COPY INTO
- **SQL to Delta** — JDBC read notebook
- **Delta to Delta** — DLT pipeline with `pipeline_task`
- Handles type mapping, column mapping, and partitioning from ADF typeProperties

### DatabricksNotebook (`notebook.py`)

Direct 1:1 mapping. Extracts:
- `notebookPath` from typeProperties
- `baseParameters` mapped to task parameters
- Linked service cluster config mapped to job cluster or existing cluster reference

### DatabricksSparkJar (`spark_jar.py`)

Maps to `spark_jar_task` with:
- `mainClassName` and `parameters` from typeProperties
- JAR library references from linked service or activity settings

### DatabricksSparkPython (`spark_python.py`)

Maps to `spark_python_task` with:
- `pythonFile` path and `parameters` from typeProperties
- Library dependencies

### ForEach (`for_each.py`)

Maps to `for_each_task` with:
- `items` expression translated to task parameter or task values reference
- `isSequential` mapped to concurrency setting
- Inner activities translated recursively

### IfCondition (`if_condition.py`)

Maps to `condition_task` with:
- `expression` translated to a condition expression
- `ifTrueActivities` and `ifFalseActivities` translated recursively as nested tasks

### SetVariable (`set_variable.py`)

Maps to a lightweight notebook that sets task values:
- Variable name becomes task value key
- Variable value expression becomes the task value

### Lookup (`lookup.py`)

Maps to a notebook that reads data and returns results via task values:
- Source dataset determines the read method (SQL query, file read, etc.)
- `firstRowOnly` setting determines output shape

### WebActivity (`web_activity.py`)

Maps to a notebook that makes HTTP requests:
- URL, method, headers, body from typeProperties
- Authentication from linked service
- Response captured as task value

### Delete (`delete.py`)

Maps to a notebook using `dbutils.fs.rm`:
- Dataset path determines the target
- `recursive` flag from typeProperties

### ExecutePipeline (`execute_pipeline.py`)

Maps to `run_job_task`:
- Referenced pipeline name mapped to target job name
- `parameters` mapped to job parameters
- `waitOnCompletion` maps to task dependency behavior

### DatabricksJob (`databricks_job.py`)

Maps to `run_job_task`:
- Existing Databricks job reference preserved
- Parameters forwarded

## Agentic Translation Notes

Agentic translations are handled by skills from the `adf-to-databricks-plugin` (`birbalin25/adf-to-databricks-plugin`). These skills use LLM reasoning to:

1. Interpret complex ADF semantics that lack direct Databricks equivalents
2. Convert ADF expressions to Python/SQL equivalents
3. Generate purpose-built notebooks for activities without task-level mappings
4. Handle data flow visual transformations (joins, pivots, derived columns, etc.)
5. Map trigger schedules accounting for timezone and windowing semantics

The agentic approach trades speed for coverage — it can handle the long tail of ADF activity types that would be impractical to build deterministic translators for.
