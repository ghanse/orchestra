# Orchestra Development Guide

ADF to Databricks Lakeflow translator via Declarative Automation Bundles.

## Commands

```bash
make dev          # Install dependencies
make test         # Unit tests
make integration  # Integration tests (requires ADF fixtures)
make fmt          # Format + lint (ruff + mypy)
make clean        # Remove build artifacts
```

## Package Structure

```
lib/orchestra/
  __init__.py
  utils.py                          # Shared utility functions
  models/                           # Shared data models
    adf_ast.py                      # Typed AST nodes for ADF definitions
    ir.py                           # Databricks intermediate representation
    dab.py                          # DAB output schema types
  parser/                           # Phase 1: ADF JSON parsing
    adf_loader.py                   # Entry point — parses ADF exports, produces inventory.json
    expression_parser.py            # ADF expression translation (@activity, @pipeline, @variables)
  translator/                       # Phase 2: ADF-to-Databricks IR translation
    engine.py                       # Entry point — registry dispatch, topological sort, context threading
    activity_translators/           # One module per deterministic activity type
      copy.py                       # Copy activity → notebook task
      notebook.py                   # DatabricksNotebook → notebook task
      spark_jar.py                  # DatabricksSparkJar → spark jar task
      spark_python.py               # DatabricksSparkPython → spark python task
      for_each.py                   # ForEach → for_each_task (control-flow, threads context)
      if_condition.py               # IfCondition → if_else_task (control-flow, threads context)
      set_variable.py               # SetVariable → run_job_task (control-flow, threads context)
      append_variable.py            # AppendVariable → run_job_task (control-flow, threads context)
      switch.py                     # Switch → if_else_task chain (control-flow, threads context)
      lookup.py                     # Lookup → notebook task
      web_activity.py               # WebActivity → notebook task
      delete.py                     # Delete → notebook task
      execute_pipeline.py           # ExecutePipeline → run_job_task
      databricks_job.py             # DatabricksJob → run_job_task
      wait.py                       # Wait → notebook task (sleep)
      filter.py                     # Filter → notebook task
  preparer/                         # Phase 2.5: Activity-specific preparation
    workflow_preparer.py            # Orchestrates activity preparers
    code_generator.py               # Notebook code generation for activity types
    activity_preparers/             # One module per activity type
      copy.py                       # Copy activity preparation
      notebook.py                   # Notebook activity preparation
      spark_jar.py                  # SparkJar activity preparation
      spark_python.py               # SparkPython activity preparation
      for_each.py                   # ForEach preparation (recursive)
      if_condition.py               # IfCondition preparation (recursive)
      set_variable.py               # SetVariable preparation
      append_variable.py            # AppendVariable preparation
      switch.py                     # Switch preparation (recursive)
      lookup.py                     # Lookup preparation
      web_activity.py               # WebActivity preparation
      delete.py                     # Delete preparation
      execute_pipeline.py           # ExecutePipeline preparation
      databricks_job.py             # DatabricksJob preparation
      wait.py                       # Wait preparation
      filter.py                     # Filter preparation
  bundler/                          # Phase 3: DABs project generation
    dab_writer.py                   # Entry point — generates databricks.yml, job YAML, resources
    notebook_writer.py              # Writes generated notebooks to bundle
    setup_generator.py              # Generates setup scripts for UC volumes, secrets, connections

skills/                             # Claude Code plugin skill definitions
  ingest/SKILL.md                   # Parse ADF JSON, produce inventory
  translate/SKILL.md                # Deterministic + agentic translation
  prepare/SKILL.md                  # Generate DABs project
  migrate/SKILL.md                  # End-to-end orchestration (ingest -> translate -> prepare)

tests/
  unit/                             # Unit tests for translators, parser, bundler
  integration/                      # Integration tests requiring ADF fixtures
  resources/json/                   # Sample ADF JSON files for testing
    pipelines/
    datasets/
    linked_services/
    triggers/

templates/                          # Jinja2 or string templates for generated notebooks and YAML
```

## Architecture

### Three-Phase Pipeline
1. **Ingest** -- Parse ADF JSON from UC volumes -> typed AST -> inventory.json
2. **Translate** -- Registry dispatch + topological sort -> Pipeline IR (deterministic + agentic gaps)
3. **Prepare** -- IR -> DAB YAML + generated notebooks + setup scripts

### Key Patterns
- `@dataclass(slots=True, kw_only=True)` for all models
- Immutable `TranslationContext` threaded through visitors
- Registry-based dispatch with match statement for control-flow types
- `TranslationStrategy` enum: DETERMINISTIC > AGENTIC > UNSUPPORTED

### Deterministic Types (16)
Copy, DatabricksNotebook, DatabricksSparkJar, DatabricksSparkPython, ForEach, IfCondition, SetVariable, Lookup, WebActivity, Delete, ExecutePipeline, DatabricksJob, Switch, Wait, Filter, AppendVariable

### Agentic Fallback Types (12)
ExecuteDataFlow, SqlServerStoredProcedure, AzureFunction, WebHook, Custom, ExecuteSSISPackage, AzureMLExecutePipeline, GetMetadata, Validation, Fail, Script, Until

## Adding a New Deterministic Translator
1. Add IR dataclass to `lib/orchestra/models/ir.py`
2. Create translator at `lib/orchestra/translator/activity_translators/<type>.py`
3. Create preparer at `lib/orchestra/preparer/activity_preparers/<type>.py`
4. Add notebook generator to `lib/orchestra/preparer/code_generator.py` if needed
5. Register in engine.py (TRANSLATOR_REGISTRY for leaf, match statement for control-flow)
6. Move from AGENTIC_TYPES to DETERMINISTIC_TYPES in adf_loader.py
7. Update activity-mapping.md reference
8. Add test fixtures and unit tests

## Critical Rules
- Never modify TranslationContext in place -- always return a new instance
- Control-flow types (ForEach, IfCondition, Switch, SetVariable, AppendVariable) thread context
- Leaf types return Activity only, control-flow returns (Activity, TranslationContext)
- Use `parse_expression()` for ADF expression translation, return None for unsupported
