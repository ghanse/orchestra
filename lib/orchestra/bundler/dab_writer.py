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
import sys
from pathlib import Path
from typing import Any

import yaml

from orchestra.bundler.notebook_writer import write_notebooks
from orchestra.bundler.setup_generator import generate_setup_tasks
from orchestra.models.dab import DabNotebook
from orchestra.preparer.workflow_preparer import PreparedWorkflow
from orchestra.utils import normalize_task_key


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
    return {
        "resources": {
            "jobs": {
                resource_key: {
                    "name": workflow.name,
                    "tasks": workflow.tasks,
                },
            },
        },
    }


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
        yaml.dump(databricks_yml_dict, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    created_files.append(databricks_yml_path.resolve())

    # 2. Write job resource YAML
    resources_dir = output_dir / "resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    job_yml_path = resources_dir / f"{resource_key}.yml"
    job_resource = _build_job_resource(workflow, resource_key)
    job_yml_path.write_text(
        yaml.dump(job_resource, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    created_files.append(job_yml_path.resolve())

    # Write inner workflows as additional resource files
    for inner in workflow.inner_workflows:
        inner_key = normalize_task_key(inner.name)
        inner_yml_path = resources_dir / f"{inner_key}.yml"
        inner_resource = _build_job_resource(inner, inner_key)
        inner_yml_path.write_text(
            yaml.dump(inner_resource, default_flow_style=False, sort_keys=False, allow_unicode=True),
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

    return created_files


def _load_report(report_path: Path) -> list[PreparedWorkflow]:
    """Load a translation report and reconstruct PreparedWorkflow objects.

    Reads the translation_report.json or pipeline IR JSON format produced by the
    translate phase and builds PreparedWorkflow objects suitable for the bundle writer.

    Args:
        report_path: Path to the translation report JSON file.

    Returns:
        List of PreparedWorkflow objects, one per pipeline.
    """
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)

    workflows: list[PreparedWorkflow] = []

    # Handle single-pipeline IR format (output of engine.py)
    if "tasks" in report and "name" in report:
        wf = _pipeline_dict_to_workflow(report)
        workflows.append(wf)
        return workflows

    # Handle translation_report.json format with translations list
    if "translations" in report:
        pipelines: dict[str, list[dict]] = {}
        for t in report.get("translations", []):
            pipeline_name = t.get("pipeline", "unknown")
            pipelines.setdefault(pipeline_name, []).append(t)

        for pipeline_name, translations in pipelines.items():
            tasks: list[dict[str, Any]] = []
            notebooks: list[DabNotebook] = []

            for t in translations:
                if t.get("status") != "translated":
                    continue
                ir = t.get("ir", {})
                if not ir:
                    continue

                task: dict[str, Any] = {"task_key": ir.get("task_key", "")}
                task_type = ir.get("task_type", "notebook_task")

                if task_type == "notebook_task":
                    notebook_path = ir.get("notebook_path", "")
                    nb_path = f"../src/{notebook_path}" if not notebook_path.startswith("../") else notebook_path
                    task["notebook_task"] = {"notebook_path": nb_path}
                    if ir.get("parameters"):
                        task["notebook_task"]["base_parameters"] = ir["parameters"]

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

    for task_ir in pipeline_dict.get("tasks", []):
        task: dict[str, Any] = {"task_key": task_ir.get("task_key", "")}
        task_type_name = task_ir.get("type", "")

        if task_ir.get("depends_on"):
            task["depends_on"] = [{"task_key": d["task_key"]} for d in task_ir["depends_on"]]
        if task_ir.get("timeout_seconds"):
            task["timeout_seconds"] = task_ir["timeout_seconds"]
        if task_ir.get("max_retries"):
            task["max_retries"] = task_ir["max_retries"]

        # Map IR type to DAB task type
        if task_type_name == "NotebookActivity":
            task["notebook_task"] = {
                "notebook_path": task_ir.get("notebook_path", ""),
            }
            if task_ir.get("base_parameters"):
                task["notebook_task"]["base_parameters"] = task_ir["base_parameters"]

        elif task_type_name == "SparkJarActivity":
            task["spark_jar_task"] = {
                "main_class_name": task_ir.get("main_class_name", ""),
            }
            if task_ir.get("parameters"):
                task["spark_jar_task"]["parameters"] = task_ir["parameters"]
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
                run_job["job_parameters"] = {k: str(v) for k, v in task_ir["parameters"].items()}
            task["run_job_task"] = run_job

        elif task_type_name in ("PlaceholderActivity", "UnsupportedActivity"):
            nb_path = task_ir.get("notebook_path", f"../src/notebooks/{task_ir.get('task_key', 'placeholder')}.py")
            task["notebook_task"] = {"notebook_path": nb_path}

        else:
            # Generic notebook task fallback
            task["notebook_task"] = {
                "notebook_path": f"../src/notebooks/{task_ir.get('task_key', 'unknown')}.py",
            }

        tasks.append(task)

    return PreparedWorkflow(
        name=pipeline_dict.get("name", "unknown"),
        tasks=tasks,
        notebooks=notebooks,
        secrets=[],
        setup_tasks=[],
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


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
    workflows = _load_report(args.report)

    if not workflows:
        print("No translated pipelines found in the report.", file=sys.stderr)
        sys.exit(1)

    all_created: list[Path] = []
    for i, workflow in enumerate(workflows):
        # If multiple workflows, create sub-directories
        if len(workflows) > 1:
            wf_dir = args.output_dir / normalize_task_key(workflow.name)
        else:
            wf_dir = args.output_dir

        bundle_name = args.bundle_name if len(workflows) == 1 else None
        created = write_bundle(
            workflow=workflow,
            output_dir=wf_dir,
            catalog=args.catalog,
            schema=args.schema,
            bundle_name=bundle_name,
        )
        all_created.extend(created)
        print(f"  [{i + 1}/{len(workflows)}] {workflow.name}: {len(created)} files")

    print(f"\nBundle generation complete: {len(all_created)} files written to {args.output_dir}")
    print("\nNext steps:")
    print("  1. Review the generated notebooks in src/")
    print("  2. Run the setup notebooks to create secrets and volumes")
    print("  3. Validate the bundle: databricks bundle validate")
    print("  4. Deploy: databricks bundle deploy -t dev")


if __name__ == "__main__":
    main()
