# Orchestra Changelog

All notable changes to Orchestra will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Motif detection and collapsing system: recognise repeated activity sub-graphs and emit a single `MotifActivity` per match, with a generated motif notebook for each replacement type.
- All 84 ADF expression functions in `parser/expression_parser.py`, with a unified `ExpressionResult` carrying `kind` (`literal` / `dab_ref` / `notebook_code`) and `imports`.
- `PreparedArtifacts` frozen dataclass and `merge_prepared_artifacts` for immutable accumulation of notebooks, secrets, setup tasks, and inner workflows across activity preparers.
- Filter activity now pre-translates its condition at translate time (`FilterActivity.condition_code` / `condition_imports`), with a placeholder body fallback when the expression cannot be safely lowered.
- Sink-side UC volume `SetupTask` emission for Copy activities targeting cloud storage (location, location_type, storage_account threaded into bundle setup).
- `lib/AGENTS.md` style guide covering naming, docstrings, comments, imports, mutability, function shape, duplication, tests, and tooling.
- Helper-coverage unit tests for shared preparer helpers, source-type taxonomy, `PreparedArtifacts` immutability, and expression parser handler factories.
- `tests/integration/` regression coverage for the legacy aggregated `translation_report.json` reload path.

### Changed
- DAB reload path collapsed: `_pipeline_dict_to_workflow` now rehydrates IR via `_reconstruct_ir` and routes everything through `prepare_workflow`, eliminating the parallel implementation in `dab_writer.py` (~1100 lines removed).
- Promoted `_naming.py`, `_helpers.py`, and `_resolve.py` to public modules under `preparer/activity_preparers/` and `translator/activity_translators/`; cross-module private imports are no longer used.
- Renamed `PreparedAggregates` → `PreparedArtifacts` for clarity; `merge_*` is now non-mutating and returns a new instance.
- Lookup query embedding now uses `repr()` for static strings and AST validation for dynamic, replacing unsafe interpolation.
- Switch first-case naming changed to `<switch>_case_<value>` for consistency with subsequent cases.
- Generated bundles emit `job_clusters` only when classic-compute tasks exist; the serverless `environments` block is omitted when no library installs are needed.
- IfCondition outcome dependency injection is now idempotent (prepends rather than overwrites).
- Motif collapser uses sanitised `task_key` consistently when computing external dependencies and rewiring.
- Library-wide docstring and comment cleanup: third-person present-indicative docstrings, no narrating comments, no abbreviated identifiers.

### Fixed
- `HttpSource` is now classified only as `FILE_SOURCE_TYPES`, not `REST_SOURCE_TYPES` (HttpSource downloads a single file; `RestSource` is the paginated-API path).
- `AzureSqlDatabaseSource` (the v2 type name) added to `JDBC_SOURCE_TYPES` alongside `AzureSqlSource`.
- Filter notebook generation no longer uses `eval()` on raw ADF expressions; widget references inside the lowered condition are post-processed to `item.get('X')`.
- Stale `_placeholder_notebook` reference in the legacy aggregated `translation_report.json` branch of `_load_report`.
- Existing-notebook activities preserve raw `@expression` strings in `base_parameters` for manual handling instead of injecting unresolved `dab_ref` placeholders.
- Per-pipeline secret scopes now use the job/pipeline name; `{{input}}` references inside `for_each_task` resolve correctly; `retry_on_timeout` is propagated; placeholder activities retain their original ADF type.
- Container-level UC volumes and volume-based checkpoints for streaming sinks.

## [0.2.0] - 2026-04-09

### Added
- Deterministic translators for Switch, Wait, Filter, AppendVariable activities
- 16 deterministic activity types now supported
- Integration tests with comprehensive ADF fixture coverage

## [0.1.0] - 2026-04-09

### Added
- Initial plugin with 4 skills: ingest, translate, prepare, migrate
- 12 deterministic activity translators (Copy, Notebook, SparkJar, SparkPython, ForEach, IfCondition, SetVariable, Lookup, WebActivity, Delete, ExecutePipeline, DatabricksJob)
- 12 agentic fallback types via adf-to-databricks-plugin
- ADF JSON parser with ARM template normalization
- Expression parser for activity outputs, pipeline variables, variable references
- DAB bundle writer generating databricks.yml + job YAML + notebooks
- Setup generator for UC volumes, secrets, connections
- Typed AST models for ADF definitions
- Immutable IR with context-threading translation engine
- Topological sort for dependency-first activity ordering
