# Orchestra Changelog

All notable changes to Orchestra will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
