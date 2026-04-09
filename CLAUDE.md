# Orchestra Development Guide

ADF to Databricks Lakeflow Jobs translator. Converts Azure Data Factory pipeline definitions into Databricks Declarative Automation Bundles (DABs).

## Commands

```bash
# Run tests
python -m pytest tests/

# Format code
ruff format lib/ tests/

# Lint
ruff check lib/ tests/

# Type check
mypy lib/orchestra/
```

## Package Structure

```
lib/orchestra/
  parser/           # Phase 1: ADF JSON parsing and inventory classification
    adf_loader.py   # Entry point — parses ADF exports, produces inventory.json
  translator/       # Phase 2: ADF-to-Databricks IR translation
    engine.py        # Entry point — runs deterministic translators, merges agentic results
    activity_translators/  # One module per deterministic activity type
  bundler/          # Phase 3: DABs project generation
    dab_writer.py    # Entry point — generates databricks.yml, resources, notebooks
  models/           # Shared data models (AST nodes, IR types, inventory schema)
  preparer/         # Activity-specific preparation logic
    activity_preparers/

skills/             # Claude Code plugin skill definitions
  ingest/           # Parse ADF JSON, produce inventory
  translate/        # Deterministic + agentic translation
  prepare/          # Generate DABs project
  migrate/          # End-to-end orchestration (ingest -> translate -> prepare)

tests/
  unit/             # Unit tests for translators, parser, bundler
  resources/json/   # Sample ADF JSON files for testing
    pipelines/
    datasets/
    linked_services/
    triggers/

templates/          # Jinja2 or string templates for generated notebooks and YAML
```

## Design Decisions

1. **Deterministic-first, agentic fallback** — Well-known ADF activity types (Copy, DatabricksNotebook, ForEach, IfCondition, etc.) are translated by deterministic Python modules. Complex or rare types fall back to LLM-assisted skills from the `adf-to-databricks-plugin`. This maximizes speed, reliability, and testability while still covering the long tail.

2. **Three-phase pipeline** — Ingest, Translate, Prepare. Each phase produces a persistent artifact (inventory.json, translation_report.json, DABs project) enabling resumability, inspection, and independent execution.

3. **IR as decoupling layer** — A Databricks intermediate representation sits between ADF parsing and DABs generation. This separates semantic translation from output serialization.

4. **DABs output** — The final output is a standard Databricks Declarative Automation Bundles project, deployable via `databricks bundle validate/deploy`. One Databricks Job per ADF pipeline, with tasks mapped from activities.

5. **Agentic delegation** — Agentic translation uses skills from `birbalin25/adf-to-databricks-plugin` (adf-dataflow-converter, adf-pipeline-converter, adf-expression-translator, adf-trigger-converter). Orchestra orchestrates these skills, passing context and merging results.

## Adding a New Deterministic Translator

1. Create `lib/orchestra/translator/activity_translators/<activity_type>.py`
2. Implement the `translate(activity: dict, context: TranslationContext) -> IR` function
3. Register the activity type in the engine's type-to-translator mapping
4. Add the type to the `DETERMINISTIC_TYPES` set in the parser's classifier
5. Add unit tests in `tests/unit/test_<activity_type>.py`
6. Update `skills/translate/references/activity-mapping.md`
