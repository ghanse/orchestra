"""Write a PreparedWorkflow to Databricks Declarative Automation Bundle files.

Produces a complete DAB directory structure that ``databricks bundle validate``
will accept:

::

    <output_dir>/
        databricks.yml
        resources/
            <job_key>.yml
        src/
            notebooks/
                <name>.py
            setup/
                <name>.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from orchestra.bundler.inner_job_params import (
    collect_inner_job_params,
    normalize_inner_task_params,
)
from orchestra.bundler.notebook_writer import write_notebooks
from orchestra.bundler.setup_generator import generate_setup_tasks
from orchestra.models.dab import DabNotebook
from orchestra.models.ir import (
    AppendVariableActivity,
    CopyActivity,
    DeleteActivity,
    FilterActivity,
    LookupActivity,
    SetVariableActivity,
    WaitActivity,
)
from orchestra.models.ir import (
    WebActivity as WebActivityIR,
)
from orchestra.preparer.code_generator import (
    generate_append_variable_notebook,
    generate_copy_notebook,
    generate_delete_notebook,
    generate_filter_notebook,
    generate_lookup_notebook,
    generate_set_variable_notebook,
    generate_wait_notebook,
    generate_web_activity_notebook,
)
from orchestra.preparer.workflow_preparer import PreparedWorkflow
from orchestra.utils import normalize_task_key


class _SingleQuotedDumper(yaml.SafeDumper):
    """YAML dumper that single-quotes all string scalar values."""


def _str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    """Force single-quoted style for all strings."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")


_SingleQuotedDumper.add_representer(str, _str_representer)

# Module-level warnings collector — reset per write_bundle call.
_bundle_warnings: list[str] = []


def write_bundle(
    workflow: PreparedWorkflow,
    output_dir: Path,
    catalog: str = "main",
    schema: str = "default",
    bundle_name: str | None = None,
) -> list[Path]:
    """Write all DAB files to output_dir.

    Creates the following directory structure::

        <output_dir>/
            databricks.yml
            resources/<job_key>.yml
            src/notebooks/<name>.py
            src/setup/<name>.py

    Args:
        workflow: The PreparedWorkflow to serialize.
        output_dir: Root directory for the bundle output.
        catalog: Default target catalog name.
        schema: Default target schema name.
        bundle_name: Optional bundle name (defaults to workflow name).

    Returns:
        List of absolute paths to all created files.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    created_files: list[Path] = []
    resource_key = normalize_task_key(workflow.name)
    effective_name = bundle_name or resource_key

    # 1. Write databricks.yml
    databricks_yml_path = output_dir / "databricks.yml"
    databricks_yml_dict = _build_databricks_yml(effective_name, catalog, schema)
    databricks_yml_path.write_text(
        yaml.dump(
            databricks_yml_dict,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            Dumper=_SingleQuotedDumper,
        ),
        encoding="utf-8",
    )
    created_files.append(databricks_yml_path.resolve())

    # 2. Write job resource YAML
    resources_dir = output_dir / "resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    job_yml_path = resources_dir / f"{resource_key}.yml"
    job_resource = _build_job_resource(workflow, resource_key)
    job_yml_path.write_text(
        yaml.dump(
            job_resource, default_flow_style=False, sort_keys=False, allow_unicode=True, Dumper=_SingleQuotedDumper
        ),
        encoding="utf-8",
    )
    created_files.append(job_yml_path.resolve())

    # Write inner workflows as additional resource files
    for inner in workflow.inner_workflows:
        inner_key = normalize_task_key(inner.name)
        inner_yml_path = resources_dir / f"{inner_key}.yml"
        inner_resource = _build_job_resource(inner, inner_key)
        inner_yml_path.write_text(
            yaml.dump(
                inner_resource,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
                Dumper=_SingleQuotedDumper,
            ),
            encoding="utf-8",
        )
        created_files.append(inner_yml_path.resolve())

    # 3. Write generated notebooks
    src_dir = output_dir / "src"
    if workflow.notebooks:
        created_files.extend(write_notebooks(workflow.notebooks, src_dir))

    # 4. Generate and write setup notebooks
    setup_notebooks: list[DabNotebook] = generate_setup_tasks(
        secrets=workflow.secrets,
        setup_tasks=workflow.setup_tasks,
        catalog=catalog,
        schema=schema,
    )
    if setup_notebooks:
        created_files.extend(write_notebooks(setup_notebooks, src_dir))

    # Collect notebooks from inner workflows
    for inner in workflow.inner_workflows:
        if inner.notebooks:
            created_files.extend(write_notebooks(inner.notebooks, src_dir))
        inner_setup = generate_setup_tasks(
            secrets=inner.secrets,
            setup_tasks=inner.setup_tasks,
            catalog=catalog,
            schema=schema,
        )
        if inner_setup:
            created_files.extend(write_notebooks(inner_setup, src_dir))

    # 5. Write warnings file if any warnings were collected
    if _bundle_warnings:
        warnings_path = output_dir / "WARNINGS.md"
        lines = [
            "# Translation Warnings\n",
            "",
            "The following items require manual review or modification:\n",
            "",
        ]
        lines.extend(_bundle_warnings)
        lines.append("")
        warnings_path.write_text("\n".join(lines), encoding="utf-8")
        created_files.append(warnings_path.resolve())

    return created_files


def main() -> None:
    """CLI entry point for DAB bundle generation.

    Accepts a translation report and writes a complete DAB bundle to
    the specified output directory.
    """
    parser = argparse.ArgumentParser(
        description="Generate a Databricks Declarative Automation Bundle from a translation report.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        required=True,
        help="Path to the translation report or pipeline IR JSON produced by the translate phase.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./orchestra_output/bundle"),
        help="Output directory for the DAB bundle (default: ./orchestra_output/bundle).",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="main",
        help="Target Unity Catalog name (default: main).",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="default",
        help="Target schema name (default: default).",
    )
    parser.add_argument(
        "--bundle-name",
        type=str,
        default=None,
        help="Override the bundle name (defaults to the workflow name).",
    )
    args = parser.parse_args()

    if not args.report.exists():
        print(f"Error: Report file not found: {args.report}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading translation report: {args.report}")
    _bundle_warnings.clear()
    workflows = _load_report(args.report)

    if not workflows:
        print("No translated pipelines found in the report.", file=sys.stderr)
        sys.exit(1)

    all_created: list[Path] = []
    for index, workflow in enumerate(workflows):
        # If multiple workflows, create sub-directories
        if len(workflows) > 1:
            workflow_dir = args.output_dir / normalize_task_key(workflow.name)
        else:
            workflow_dir = args.output_dir

        effective_bundle_name = args.bundle_name if len(workflows) == 1 else None
        created = write_bundle(
            workflow=workflow,
            output_dir=workflow_dir,
            catalog=args.catalog,
            schema=args.schema,
            bundle_name=effective_bundle_name,
        )
        all_created.extend(created)
        print(f"  [{index + 1}/{len(workflows)}] {workflow.name}: {len(created)} files")

    print(f"\nBundle generation complete: {len(all_created)} files written to {args.output_dir}")
    print("\nNext steps:")
    print("  1. Review the generated notebooks in src/")
    print("  2. Run the setup notebooks to create secrets and volumes")
    print("  3. Validate the bundle: databricks bundle validate")
    print("  4. Deploy: databricks bundle deploy -t dev")


# Serialization helpers


def _warn(task_key: str, message: str) -> None:
    """Record a translation warning for the current bundle."""
    _bundle_warnings.append(f"- **{task_key}**: {message}")


def _build_databricks_yml(
    bundle_name: str,
    catalog: str,
    schema: str,
) -> dict[str, Any]:
    """Build the root ``databricks.yml`` configuration as a dict.

    Args:
        bundle_name: Name for the bundle.
        catalog: Default target catalog.
        schema: Default target schema.

    Returns:
        Dict ready for YAML serialization.
    """
    return {
        "bundle": {
            "name": bundle_name,
        },
        "variables": {
            "catalog": {
                "description": "Target catalog",
                "default": catalog,
            },
            "schema": {
                "description": "Target schema",
                "default": schema,
            },
        },
        "include": [
            "resources/*.yml",
        ],
        "targets": {
            "dev": {
                "mode": "development",
            },
            "staging": {
                "mode": "development",
            },
            "prod": {
                "mode": "production",
            },
        },
    }


def _build_job_resource(
    workflow: PreparedWorkflow,
    resource_key: str,
) -> dict[str, Any]:
    """Build a job resource dict for a single workflow.

    Args:
        workflow: The prepared workflow to serialize.
        resource_key: The sanitised resource key for this job.

    Returns:
        Dict ready for YAML serialization.
    """
    job_def: dict[str, Any] = {
        "name": workflow.name,
        "tasks": workflow.tasks,
    }

    if workflow.parameters:
        job_def["parameters"] = workflow.parameters

    return {
        "resources": {
            "jobs": {
                resource_key: job_def,
            },
        },
    }


def _normalize_base_parameters(
    params: dict[str, Any],
    *,
    task_key: str = "",
) -> dict[str, str]:
    """Normalise raw ADF expression dicts in base_parameters to resolved strings.

    Handles ``{type: Expression, value: '@...'}`` dicts and plain strings.
    Uses the same resolver as inner job params so ``@pipeline().parameters.X``
    becomes ``{{job.parameters.X}}`` and ``@concat(...)`` is flattened.

    When a resolved value contains Python code (e.g. ``dbutils.widgets.get``
    concatenation), it cannot be used as a DAB parameter string.  A warning
    is emitted and the value is kept as-is for manual review.

    Args:
        params: Raw base_parameters dict from the IR.
        task_key: Task key for warning attribution.

    Returns:
        Dict with all values resolved to strings.
    """
    from orchestra.bundler.inner_job_params import _normalize_value

    resolved: dict[str, str] = {}
    for key, value in params.items():
        normalized = _normalize_value(value)
        if "dbutils.widgets.get" in normalized or "dbutils.jobs.taskValues" in normalized:
            _warn(
                task_key,
                f"Parameter `{key}` contains a computed expression that cannot be "
                f"expressed as a DAB dynamic value reference. The task's notebook "
                f"or entry point must handle this parameter at runtime. "
                f"Value: `{normalized}`",
            )
        resolved[key] = normalized
    return resolved


def _normalize_value_import(value: Any) -> str:
    """Thin wrapper to normalise a value — avoids circular import issues.

    Delegates to :func:`_normalize_value` from the inner_job_params module
    that is already imported at module level.
    """
    from orchestra.bundler.inner_job_params import _normalize_value

    return _normalize_value(value)


# Report loading and IR reconstruction


def _load_report(report_path: Path) -> list[PreparedWorkflow]:
    """Load a translation report and reconstruct PreparedWorkflow objects.

    Reads the translation_report.json or pipeline IR JSON format produced by the
    translate phase and builds PreparedWorkflow objects suitable for the bundle writer.

    Args:
        report_path: Path to the translation report JSON file.

    Returns:
        List of PreparedWorkflow objects, one per pipeline.
    """
    with open(report_path, encoding="utf-8") as report_file:
        report = json.load(report_file)

    workflows: list[PreparedWorkflow] = []

    # Handle single-pipeline IR format (output of engine.py)
    if "tasks" in report and "name" in report:
        workflow = _pipeline_dict_to_workflow(report)
        workflows.append(workflow)
        return workflows

    # Handle translation_report.json format with translations list
    if "translations" in report:
        pipelines: dict[str, list[dict]] = {}
        for translation in report.get("translations", []):
            pipeline_name = translation.get("pipeline", "unknown")
            pipelines.setdefault(pipeline_name, []).append(translation)

        for pipeline_name, translations in pipelines.items():
            tasks: list[dict[str, Any]] = []
            notebooks: list[DabNotebook] = []

            for translation in translations:
                if translation.get("status") != "translated":
                    continue
                ir = translation.get("ir", {})
                if not ir:
                    continue

                task_key = ir.get("task_key", "")
                activity_name = ir.get("name", task_key)
                task: dict[str, Any] = {"task_key": task_key}
                task_type = ir.get("task_type", "notebook_task")

                if task_type == "notebook_task":
                    notebook_path = ir.get("notebook_path", "")
                    notebook_relative_path = (
                        notebook_path if not notebook_path.startswith("../") else notebook_path[len("../src/") :]
                    )
                    resolved_notebook_path = (
                        f"../src/{notebook_relative_path}" if not notebook_path.startswith("../") else notebook_path
                    )
                    task["notebook_task"] = {"notebook_path": resolved_notebook_path}
                    if ir.get("parameters"):
                        task["notebook_task"]["base_parameters"] = ir["parameters"]
                    notebooks.append(
                        DabNotebook(
                            relative_path=notebook_relative_path,
                            content=_placeholder_notebook(task_key, activity_name, task_type),
                        )
                    )

                tasks.append(task)

            workflows.append(
                PreparedWorkflow(
                    name=pipeline_name,
                    tasks=tasks,
                    notebooks=notebooks,
                    secrets=[],
                    setup_tasks=[],
                )
            )

    return workflows


def _pipeline_dict_to_workflow(pipeline_dict: dict[str, Any]) -> PreparedWorkflow:
    """Convert a serialized pipeline IR dict to a PreparedWorkflow.

    Args:
        pipeline_dict: Dict loaded from a pipeline IR JSON file.

    Returns:
        A PreparedWorkflow with tasks extracted from the IR.
    """
    tasks: list[dict[str, Any]] = []
    notebooks: list[DabNotebook] = []
    inner_workflows: list[PreparedWorkflow] = []

    for task_ir in pipeline_dict.get("tasks", []):
        result = _task_ir_to_dab(task_ir)
        tasks.append(result["task"])
        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])

    # Carry pipeline-level parameters through to the job definition
    parameters: list[dict[str, Any]] = []
    for param in pipeline_dict.get("parameters") or []:
        param_entry: dict[str, Any] = {"name": param["name"]}
        if "default" in param and param["default"] is not None:
            param_entry["default"] = str(param["default"])
        parameters.append(param_entry)

    return PreparedWorkflow(
        name=pipeline_dict.get("name", "unknown"),
        tasks=tasks,
        notebooks=notebooks,
        secrets=[],
        setup_tasks=[],
        inner_workflows=inner_workflows,
        parameters=parameters,
    )


def _reconstruct_ir(task_ir: dict[str, Any]) -> Any:
    """Reconstruct a typed IR activity object from a serialized dict.

    Builds the minimal IR dataclass needed by the code generators.  Only
    fields used by generators are populated; ``depends_on`` and ``cluster``
    are left at their defaults.

    Args:
        task_ir: Serialized IR task dict from the translate phase JSON.

    Returns:
        A typed IR activity instance, or ``None`` if the type is not
        recognised.
    """
    task_key = task_ir.get("task_key", "")
    name = task_ir.get("name", task_key)
    timeout = task_ir.get("timeout_seconds")
    max_retries = task_ir.get("max_retries")
    task_type = task_ir.get("type", "")

    base = dict(name=name, task_key=task_key, timeout_seconds=timeout, max_retries=max_retries)

    if task_type == "LookupActivity":
        return LookupActivity(
            **base,
            source_type=task_ir.get("source_type"),
            source_properties=task_ir.get("source_properties"),
            first_row_only=task_ir.get("first_row_only", True),
            source_query=task_ir.get("source_query"),
        )
    if task_type == "CopyActivity":
        return CopyActivity(
            **base,
            source_type=task_ir.get("source_type"),
            sink_type=task_ir.get("sink_type"),
            source_properties=task_ir.get("source_properties"),
            sink_properties=task_ir.get("sink_properties"),
            column_mapping=task_ir.get("column_mapping"),
        )
    if task_type == "WebActivity":
        return WebActivityIR(
            **base,
            url=task_ir.get("url", ""),
            method=task_ir.get("method", "GET"),
            body=task_ir.get("body"),
            headers=task_ir.get("headers"),
            authentication=task_ir.get("authentication"),
        )
    if task_type == "SetVariableActivity":
        return SetVariableActivity(
            **base,
            variable_name=task_ir.get("variable_name", ""),
            variable_value=task_ir.get("variable_value", ""),
            value_kind=task_ir.get("value_kind", "literal"),
            notebook_code=task_ir.get("notebook_code"),
            notebook_imports=task_ir.get("notebook_imports", []),
        )
    if task_type == "WaitActivity":
        return WaitActivity(
            **base,
            wait_time_seconds=task_ir.get("wait_time_seconds", 0),
        )
    if task_type == "DeleteActivity":
        return DeleteActivity(
            **base,
            dataset_name=task_ir.get("dataset_name", ""),
            folder_path=task_ir.get("folder_path"),
            recursive=task_ir.get("recursive", True),
        )
    if task_type == "FilterActivity":
        return FilterActivity(
            **base,
            items_expression=task_ir.get("items_expression", ""),
            condition_expression=task_ir.get("condition_expression", ""),
        )
    if task_type == "AppendVariableActivity":
        return AppendVariableActivity(
            **base,
            variable_name=task_ir.get("variable_name", ""),
            append_value=task_ir.get("append_value", ""),
            value_kind=task_ir.get("value_kind", "literal"),
            notebook_code=task_ir.get("notebook_code"),
            notebook_imports=task_ir.get("notebook_imports", []),
        )
    return None


# Notebook content generators


def _placeholder_notebook(task_key: str, activity_name: str, activity_type: str, original_path: str = "") -> str:
    """Generate placeholder notebook content for the JSON reload path.

    When loading from a serialized translation report, we don't have access to
    the rich IR objects or code generators.  This produces a minimal placeholder
    that identifies the source activity and provides next-step instructions.

    Args:
        task_key: The sanitized task key used for the notebook filename.
        activity_name: The original ADF activity name.
        activity_type: The IR activity type (e.g. ``"SetVariableActivity"``).
        original_path: The original workspace notebook path, if any.

    Returns:
        Placeholder notebook content as a string.
    """
    lines = [
        "# Databricks notebook source",
        "# MAGIC %md",
        f"# MAGIC # {activity_name}",
        "# MAGIC",
        f"# MAGIC **ADF activity type**: `{activity_type}`",
    ]
    if original_path:
        lines.extend(
            [
                "# MAGIC",
                f"# MAGIC **Source workspace path**: `{original_path}`",
                "# MAGIC",
                "# MAGIC Export from workspace:",
                "# MAGIC ```",
                f'# MAGIC databricks workspace export "{original_path}" --format SOURCE -o src/notebooks/{task_key}.py',
                "# MAGIC ```",
            ]
        )
    else:
        lines.extend(
            [
                "# MAGIC",
                f"# MAGIC This notebook replaces ADF `{activity_type}` activity `{activity_name}`.",
                "# MAGIC Implement the equivalent Databricks logic below.",
            ]
        )
    lines.extend(
        [
            "",
            "# COMMAND ----------",
            "",
            f"# TODO: Implement {activity_type} logic for '{activity_name}'",
            "raise NotImplementedError(",
            f'    "Implement {activity_type} logic for {activity_name}"',
            ")",
            "",
        ]
    )
    return "\n".join(lines)


def _web_activity_notebook(task_key: str, activity_name: str, task_ir: dict[str, Any]) -> str:
    """Generate a notebook that performs an HTTP request for a WebActivity.

    Produces a ready-to-run notebook using the ``requests`` library with the
    URL, method, headers, and body from the translated IR.

    Args:
        task_key: Sanitised task key for the notebook filename.
        activity_name: Original ADF activity display name.
        task_ir: The full serialised WebActivity IR dict.

    Returns:
        Notebook source content as a string.
    """
    url = task_ir.get("url", "")
    method = task_ir.get("method", "POST").upper()
    body_raw = task_ir.get("body")
    headers_raw = task_ir.get("headers")

    normalised_url = _normalize_value_import(url)

    body_code = ""
    if body_raw:
        if isinstance(body_raw, dict) and body_raw.get("type") == "Expression":
            expression = body_raw.get("value", "")
            body_code = (
                f"# ADF expression: {expression}\n# TODO: Translate the body expression to Python\npayload = {{}}"
            )
        elif isinstance(body_raw, dict):
            import json as _json

            body_code = f"payload = {_json.dumps(body_raw, indent=4)}"
        elif isinstance(body_raw, str):
            body_code = f'payload = "{body_raw}"'
        else:
            body_code = f"payload = {body_raw!r}"
    else:
        body_code = "payload = None"

    headers_code = "headers = None"
    if headers_raw and isinstance(headers_raw, dict):
        import json as _json

        headers_code = f"headers = {_json.dumps(headers_raw, indent=4)}"

    lines = [
        "# Databricks notebook source",
        "# MAGIC %md",
        f"# MAGIC # {activity_name}",
        "# MAGIC",
        f"# MAGIC **Migrated from ADF**: `WebActivity` ({method})",
        "",
        "# COMMAND ----------",
        "",
        "import requests",
        "",
        "# COMMAND ----------",
        "",
        f'url = "{normalised_url}"',
        "",
        body_code,
        "",
        headers_code,
        "",
        "# COMMAND ----------",
        "",
    ]

    if method in ("POST", "PUT", "PATCH"):
        lines.append(
            f"response = requests.{method.lower()}(url, json=payload, headers=headers, timeout=60)",
        )
    else:
        lines.append(
            f"response = requests.{method.lower()}(url, headers=headers, timeout=60)",
        )

    lines.extend(
        [
            "response.raise_for_status()",
            f'print(f"{activity_name}: HTTP {{response.status_code}}")',
            "",
        ]
    )

    return "\n".join(lines)


def _download_or_placeholder(
    workspace_path: str,
    task_key: str,
    activity_name: str,
    task_type_name: str,
) -> str:
    """Try to download a notebook from the workspace; fall back to a placeholder.

    Args:
        workspace_path: Original workspace path from the ADF definition.
        task_key: Sanitised task key (used in the placeholder filename).
        activity_name: ADF activity display name.
        task_type_name: IR activity type name.

    Returns:
        Notebook source content as a string.
    """
    if workspace_path:
        from orchestra.preparer.workspace_downloader import download_notebook

        content = download_notebook(workspace_path)
        if content is not None:
            return content

    return _placeholder_notebook(task_key, activity_name, task_type_name, workspace_path)


# Task IR to DAB conversion handlers


def _task_ir_to_dab(task_ir: dict[str, Any]) -> dict[str, Any]:
    """Convert a single serialized task IR dict to a DAB task dict.

    Returns a dict with keys ``task``, ``notebooks``, and ``inner_workflows``.

    Args:
        task_ir: Dict for a single task from the serialized IR JSON.

    Returns:
        Dict with ``task`` (DAB task dict), ``notebooks`` (list of DabNotebook),
        and ``inner_workflows`` (list of PreparedWorkflow).
    """
    task_key = task_ir.get("task_key", "")
    activity_name = task_ir.get("name", task_key)
    task: dict[str, Any] = {"task_key": task_key}
    task_type_name = task_ir.get("type", "")
    notebooks: list[DabNotebook] = []
    inner_workflows: list[PreparedWorkflow] = []

    if task_ir.get("depends_on"):
        task["depends_on"] = [{"task_key": dep["task_key"]} for dep in task_ir["depends_on"]]
    if task_ir.get("timeout_seconds"):
        task["timeout_seconds"] = task_ir["timeout_seconds"]
    if task_ir.get("max_retries"):
        task["max_retries"] = task_ir["max_retries"]
    if task_ir.get("min_retry_interval_millis"):
        task["min_retry_interval_millis"] = task_ir["min_retry_interval_millis"]

    if task_type_name == "ForEachActivity":
        _handle_for_each(task, task_ir, task_key, notebooks, inner_workflows)

    elif task_type_name == "NotebookActivity":
        original_path = task_ir.get("notebook_path", "")
        notebook_relative_path = f"notebooks/{task_key}.py"
        task["notebook_task"] = {
            "notebook_path": f"../src/{notebook_relative_path}",
        }
        if task_ir.get("base_parameters"):
            task["notebook_task"]["base_parameters"] = _normalize_base_parameters(
                task_ir["base_parameters"],
                task_key=task_key,
            )

        content = _download_or_placeholder(original_path, task_key, activity_name, task_type_name)
        notebooks.append(
            DabNotebook(
                relative_path=notebook_relative_path,
                content=content,
            )
        )

    elif task_type_name == "SparkJarActivity":
        task["spark_jar_task"] = {
            "main_class_name": task_ir.get("main_class_name", ""),
        }
        if task_ir.get("parameters"):
            resolved_params = []
            for param in task_ir["parameters"]:
                resolved_params.append(_normalize_value_import(param))
            task["spark_jar_task"]["parameters"] = resolved_params
        if task_ir.get("libraries"):
            task["libraries"] = task_ir["libraries"]

    elif task_type_name == "SparkPythonActivity":
        task["spark_python_task"] = {
            "python_file": task_ir.get("python_file", ""),
        }
        if task_ir.get("parameters"):
            task["spark_python_task"]["parameters"] = task_ir["parameters"]

    elif task_type_name in ("ExecutePipelineActivity", "RunJobActivity"):
        run_job: dict[str, Any] = {}
        if task_ir.get("existing_job_id"):
            run_job["job_id"] = task_ir["existing_job_id"]
        elif task_ir.get("pipeline_name"):
            ref_key = normalize_task_key(task_ir["pipeline_name"])
            run_job["job_id"] = f"${{resources.jobs.{ref_key}.id}}"
        elif task_ir.get("job_name"):
            run_job["job_id"] = f"${{resources.jobs.{task_ir['job_name']}.id}}"
        if task_ir.get("parameters"):
            job_params: dict[str, str] = {}
            for key, value in task_ir["parameters"].items():
                normalized = _normalize_value_import(value)
                if "dbutils.widgets.get" in normalized or "dbutils.jobs.taskValues" in normalized:
                    _warn(
                        task_key,
                        f"Parameter `{key}` contains a computed expression that cannot be "
                        f"expressed as a DAB dynamic value reference for run_job_task. "
                        f"Value: `{normalized}`",
                    )
                job_params[key] = normalized
            run_job["job_parameters"] = job_params
        task["run_job_task"] = run_job

    elif task_type_name == "SwitchActivity":
        _handle_switch(task, task_ir, task_key, notebooks, inner_workflows)

    elif task_type_name == "IfConditionActivity":
        _handle_if_condition(task, task_ir, task_key, notebooks, inner_workflows)

    elif task_type_name in (
        "WebActivity",
        "LookupActivity",
        "CopyActivity",
        "SetVariableActivity",
        "WaitActivity",
        "DeleteActivity",
        "FilterActivity",
        "AppendVariableActivity",
    ):
        notebook_relative_path = f"notebooks/{task_key}.py"
        ir_activity = _reconstruct_ir(task_ir)
        if ir_activity is not None:
            _generators: dict[str, Any] = {
                "WebActivity": generate_web_activity_notebook,
                "LookupActivity": generate_lookup_notebook,
                "CopyActivity": generate_copy_notebook,
                "SetVariableActivity": generate_set_variable_notebook,
                "WaitActivity": generate_wait_notebook,
                "DeleteActivity": generate_delete_notebook,
                "FilterActivity": generate_filter_notebook,
                "AppendVariableActivity": generate_append_variable_notebook,
            }
            generator_function = _generators[task_type_name]
            content = generator_function(ir_activity)
        else:
            content = _placeholder_notebook(task_key, activity_name, task_type_name)

        task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}

        if task_type_name == "LookupActivity":
            task["notebook_task"]["base_parameters"] = {
                "first_row_only": str(task_ir.get("first_row_only", True)).lower(),
            }
        elif task_type_name == "CopyActivity":
            base_parameters: dict[str, str] = {}
            if task_ir.get("source_type"):
                base_parameters["source_type"] = task_ir["source_type"]
            if task_ir.get("sink_type"):
                base_parameters["sink_type"] = task_ir["sink_type"]
            if base_parameters:
                task["notebook_task"]["base_parameters"] = base_parameters
        elif task_type_name == "WebActivity":
            task["notebook_task"]["base_parameters"] = {
                "url": _normalize_value_import(task_ir.get("url", "")),
                "method": task_ir.get("method", "GET"),
            }
        elif task_type_name == "SetVariableActivity":
            set_variable_params: dict[str, str] = {"variable_name": task_ir.get("variable_name", "")}
            if task_ir.get("value_kind") in ("literal", "dab_ref"):
                set_variable_params["value"] = _normalize_value_import(task_ir.get("variable_value", ""))
            task["notebook_task"]["base_parameters"] = set_variable_params

        notebooks.append(
            DabNotebook(
                relative_path=notebook_relative_path,
                content=content,
            )
        )

    elif task_type_name in ("PlaceholderActivity", "UnsupportedActivity"):
        notebook_relative_path = f"notebooks/{task_key or 'placeholder'}.py"
        task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}
        notebooks.append(
            DabNotebook(
                relative_path=notebook_relative_path,
                content=_placeholder_notebook(task_key, activity_name, task_type_name),
            )
        )

    else:
        notebook_relative_path = f"notebooks/{task_key or 'unknown'}.py"
        task["notebook_task"] = {
            "notebook_path": f"../src/{notebook_relative_path}",
        }
        notebooks.append(
            DabNotebook(
                relative_path=notebook_relative_path,
                content=_placeholder_notebook(task_key, activity_name, task_type_name),
            )
        )

    return {"task": task, "notebooks": notebooks, "inner_workflows": inner_workflows}


# Control flow handlers


def _handle_if_condition(
    task: dict[str, Any],
    task_ir: dict[str, Any],
    task_key: str,
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
) -> None:
    """Build a condition_task from a serialized IfConditionActivity IR dict.

    Recursively processes both branches into DAB task lists.

    Args:
        task: The DAB task dict being built (mutated in place).
        task_ir: Serialized IfConditionActivity dict from the IR JSON.
        task_key: Task key for the IfCondition activity.
        notebooks: Accumulator for generated notebooks.
        inner_workflows: Accumulator for inner job workflows.
    """
    condition: dict[str, Any] = {
        "op": task_ir.get("op", "EQUAL_TO"),
        "left": task_ir.get("left", ""),
        "right": task_ir.get("right", ""),
    }

    if_true_tasks: list[dict[str, Any]] = []
    for child_ir in task_ir.get("if_true_activities", []):
        result = _task_ir_to_dab(child_ir)
        if_true_tasks.append(result["task"])
        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])

    if_false_tasks: list[dict[str, Any]] = []
    for child_ir in task_ir.get("if_false_activities", []):
        result = _task_ir_to_dab(child_ir)
        if_false_tasks.append(result["task"])
        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])

    if if_true_tasks:
        condition["if_true"] = if_true_tasks
    if if_false_tasks:
        condition["if_false"] = if_false_tasks

    task["condition_task"] = condition


def _handle_switch(
    task: dict[str, Any],
    task_ir: dict[str, Any],
    task_key: str,
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
) -> None:
    """Build chained condition_tasks from a serialized SwitchActivity IR dict.

    Mirrors the logic in ``preparer/activity_preparers/switch.py``: each case
    becomes an equality check against the ``on_expression``, nested right-to-left
    so that the last case's ``if_false`` is the default branch.

    Args:
        task: The DAB task dict being built (mutated in place).
        task_ir: Serialized SwitchActivity dict from the IR JSON.
        task_key: Task key for the Switch activity.
        notebooks: Accumulator for generated notebooks.
        inner_workflows: Accumulator for inner job workflows.
    """
    on_expression = task_ir.get("on_expression", "")
    cases = task_ir.get("cases", [])
    default_activity_dicts = task_ir.get("default_activities", [])

    case_branches: list[dict[str, Any]] = []
    for case in cases:
        case_value = case.get("value", "")
        case_tasks: list[dict[str, Any]] = []
        for child_ir in case.get("activities", []):
            result = _task_ir_to_dab(child_ir)
            case_tasks.append(result["task"])
            notebooks.extend(result["notebooks"])
            inner_workflows.extend(result["inner_workflows"])
        case_branches.append({"value": case_value, "tasks": case_tasks})

    default_tasks: list[dict[str, Any]] = []
    for child_ir in default_activity_dicts:
        result = _task_ir_to_dab(child_ir)
        default_tasks.append(result["task"])
        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])

    if not case_branches:
        task["condition_task"] = {
            "op": "EQUAL_TO",
            "left": "true",
            "right": "true",
            "if_true": default_tasks,
            "if_false": [],
        }
        return

    # Build from the last case backwards, nesting if_false chains
    last = case_branches[-1]
    inner_condition: dict[str, Any] = {
        "op": "EQUAL_TO",
        "left": on_expression,
        "right": last["value"],
        "if_true": last["tasks"],
        "if_false": default_tasks,
    }
    inner_case_value = last["value"]

    for branch in reversed(case_branches[:-1]):
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", inner_case_value)
        sanitized = re.sub(r"_+", "_", sanitized).strip("_") or "empty"
        case_key = f"{task_key}__case_{sanitized}"
        inner_condition = {
            "op": "EQUAL_TO",
            "left": on_expression,
            "right": branch["value"],
            "if_true": branch["tasks"],
            "if_false": [{"task_key": case_key, "condition_task": inner_condition}],
        }
        inner_case_value = branch["value"]

    task["condition_task"] = inner_condition


def _handle_for_each(
    task: dict[str, Any],
    task_ir: dict[str, Any],
    task_key: str,
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
) -> None:
    """Build a for_each_task from a serialized ForEachActivity IR dict.

    Single inner activity: inlined directly in ``for_each_task.task``.
    Multiple inner activities: creates a self-contained inner job with its
    own parameter declarations, normalised expression references, and the
    full task graph.

    Args:
        task: The DAB task dict being built (mutated in place).
        task_ir: Serialized ForEachActivity dict from the IR JSON.
        task_key: Task key for the ForEach activity.
        notebooks: Accumulator for generated notebooks.
        inner_workflows: Accumulator for inner job workflows.
    """
    items_expression = task_ir.get("items_expression", "")
    concurrency = task_ir.get("concurrency", 20)
    inner_activity_dicts = task_ir.get("inner_activities", [])

    if len(inner_activity_dicts) == 1:
        # Single inner activity: inline it directly
        result = _task_ir_to_dab(inner_activity_dicts[0])
        inner_task = result["task"]
        inner_key = inner_task.get("task_key", task_key)
        # Inject {{input}} as the item parameter
        if "notebook_task" in inner_task:
            params = inner_task["notebook_task"].setdefault("base_parameters", {})
            params["item"] = "{{input}}"

        # Warn if the inner task is a non-code-gen notebook (placeholder) that
        # receives JSON-valued {{input}} — it can't parse sub-properties.
        inner_type = inner_activity_dicts[0].get("type", "")
        if inner_type == "NotebookActivity":
            _warn(
                inner_key,
                "This for_each inner task receives `{{input}}` as a JSON string. "
                "The notebook must parse it with `json.loads(dbutils.widgets.get('item'))` "
                "to access sub-properties like `partition_key` or `partition_value`.",
            )

        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])

        task["for_each_task"] = {
            "inputs": items_expression,
            "task": inner_task,
            "concurrency": concurrency,
        }

    elif len(inner_activity_dicts) > 1:
        # Multiple inner activities: create an inner job
        inner_job_name = f"{task_key}_inner_tasks"
        inner_job_key = normalize_task_key(inner_job_name)

        inner_tasks: list[dict[str, Any]] = []
        for inner_ir in inner_activity_dicts:
            result = _task_ir_to_dab(inner_ir)
            inner_tasks.append(result["task"])
            notebooks.extend(result["notebooks"])
            inner_workflows.extend(result["inner_workflows"])

        # Normalise ADF expressions to {{job.parameters.*}} references
        normalize_inner_task_params(inner_tasks)

        # Scan for all parameter references and build declarations + pass-through.
        # Pass the raw IR dicts so fields consumed during conversion (e.g.
        # WebActivity url/body) are also scanned.
        parameters, job_parameters = collect_inner_job_params(
            inner_tasks,
            raw_ir_tasks=inner_activity_dicts,
        )

        inner_workflow = PreparedWorkflow(
            name=inner_job_name,
            tasks=inner_tasks,
            notebooks=[],  # notebooks already collected above
            secrets=[],
            setup_tasks=[],
            parameters=parameters,
        )
        inner_workflows.append(inner_workflow)

        # The for_each body calls the inner job via run_job_task
        body_task: dict[str, Any] = {
            "task_key": f"{task_key}_iteration",
            "run_job_task": {
                "job_id": f"${{resources.jobs.{inner_job_key}.id}}",
                "job_parameters": job_parameters,
            },
        }

        task["for_each_task"] = {
            "inputs": items_expression,
            "task": body_task,
            "concurrency": concurrency,
        }

    else:
        # No inner activities — degenerate case
        task["for_each_task"] = {
            "inputs": items_expression,
            "task": {"task_key": f"{task_key}_noop"},
            "concurrency": concurrency,
        }


if __name__ == "__main__":
    main()
