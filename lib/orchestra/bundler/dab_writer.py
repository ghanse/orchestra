"""Writes a PreparedWorkflow to Databricks Declarative Automation Bundle files."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from collections.abc import Iterator
from typing import Any

import yaml

from orchestra.bundler.inner_job_params import (
    _normalize_value,
    collect_inner_job_params,
    normalize_inner_task_params,
)
from orchestra.bundler.notebook_writer import write_notebooks
from orchestra.bundler.prereqs_writer import (
    ManualParameter,
    build_prereqs,
    render_setup_md,
    scan_notebooks_for_secrets,
)
from orchestra.bundler.setup_generator import generate_setup_tasks
from orchestra.models.dab import DabNotebook, SecretInstruction, SetupTask
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
from orchestra.preparer.activity_preparers._naming import notebook_filename
from orchestra.preparer.workflow_preparer import PreparedWorkflow, run_if_from_adf_outcomes
from orchestra.preparer.workspace_downloader import download_notebook
from orchestra.utils import normalize_task_key


class _BundleYamlDumper(yaml.SafeDumper):
    """YAML dumper that leaves keys unquoted and only quotes values when needed."""


# Module-level warnings collector — reset per write_bundle call.
_bundle_warnings: list[str] = []

# Cross-bundle ExecutePipeline refs seen while translating: variable_name →
# target pipeline name.  Reset per write_bundle call and surfaced via the
# bundle's ``variables`` block + SETUP.md.
_cross_bundle_variables: dict[str, str] = {}

_WIDGET_REFERENCE = re.compile(r"""dbutils\.widgets\.get\(\s*["']([^"']+)["']\s*\)""")


def write_bundle(
    workflow: PreparedWorkflow,
    output_dir: Path,
    catalog: str = "main",
    schema: str = "default",
    bundle_name: str | None = None,
) -> list[Path]:
    """Writes all DAB files to output_dir.

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

    # Bind clusters across the parent workflow and any inner workflows up
    # front so we can decide whether the bundle needs cluster-related
    # tunables in ``databricks.yml`` at all.  Binding is idempotent, so the
    # subsequent ``_build_job_resource`` calls re-checking the same tasks is
    # harmless.
    _bind_cluster_to_notebook_tasks(workflow.tasks)
    for inner in workflow.inner_workflows:
        _bind_cluster_to_notebook_tasks(inner.tasks)
    bundle_uses_classic_cluster = _any_task_uses_classic_cluster(workflow.tasks) or any(
        _any_task_uses_classic_cluster(inner.tasks) for inner in workflow.inner_workflows
    )

    # 1. Write databricks.yml.  When at least one task runs on classic
    #    compute, defaults for spark_version / node_type_id come from the
    #    ADF linked service configs on the tasks so the emitted cluster
    #    matches the source-of-truth runtime.  When every task is
    #    serverless, those variables are omitted entirely.
    databricks_yml_path = output_dir / "databricks.yml"
    inferred_spark_version, inferred_node_type_id = _infer_bundle_cluster_defaults(workflow)
    databricks_yml_dict = _build_databricks_yml(
        effective_name,
        catalog,
        schema,
        spark_version=inferred_spark_version,
        node_type_id=inferred_node_type_id,
        include_cluster_variables=bundle_uses_classic_cluster,
    )
    databricks_yml_path.write_text(
        yaml.dump(
            databricks_yml_dict,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            Dumper=_BundleYamlDumper,
        ),
        encoding="utf-8",
    )
    created_files.append(databricks_yml_path.resolve())

    # 2. Write job resource YAML.  Strip broken base_parameters from
    #    existing-notebook tasks before serialising — these are surfaced
    #    in SETUP.md (§Existing-notebook parameter handling) further down
    #    and shouldn't ship in the YAML as malformed widget values.
    manual_parameters: list[ManualParameter] = _extract_manual_parameters_from_existing_notebook_tasks(workflow.tasks)
    for inner in workflow.inner_workflows:
        manual_parameters.extend(_extract_manual_parameters_from_existing_notebook_tasks(inner.tasks))

    resources_dir = output_dir / "resources"
    resources_dir.mkdir(parents=True, exist_ok=True)
    job_yml_path = resources_dir / f"{resource_key}.yml"
    job_resource = _build_job_resource(workflow, resource_key)
    job_yml_path.write_text(
        yaml.dump(
            job_resource, default_flow_style=False, sort_keys=False, allow_unicode=True, Dumper=_BundleYamlDumper
        ),
        encoding="utf-8",
    )
    created_files.append(job_yml_path.resolve())

    # Write inner workflows as additional resource files.  Inner tasks reuse
    # notebooks that live in the parent workflow's notebooks list, so pass
    # those in so the inner job's widget auto-augmentation can see them.
    for inner in workflow.inner_workflows:
        inner_key = normalize_task_key(inner.name)
        inner_yml_path = resources_dir / f"{inner_key}.yml"
        inner_resource = _build_job_resource(inner, inner_key, extra_notebooks_for_augment=workflow.notebooks)
        inner_yml_path.write_text(
            yaml.dump(
                inner_resource,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
                Dumper=_BundleYamlDumper,
            ),
            encoding="utf-8",
        )
        created_files.append(inner_yml_path.resolve())

    # 3. Write generated notebooks
    src_dir = output_dir / "src"
    if workflow.notebooks:
        created_files.extend(write_notebooks(workflow.notebooks, src_dir))

    # 4. Generate and write setup notebooks (create-scope, create-volume, etc.).
    #    These are the *executable* provisioning artifacts; SETUP.md (below)
    #    is the human-readable companion.
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

    # 5. Build SETUP.md — a root-level, human-readable summary of every
    #    external step the user must take before ``bundle run``.  This is
    #    additive to the setup/ notebooks above; the setup notebooks are
    #    the executable path, SETUP.md is the checklist.
    all_notebooks = list(workflow.notebooks)
    for inner in workflow.inner_workflows:
        all_notebooks.extend(inner.notebooks)
    all_tasks = list(workflow.tasks)
    for inner in workflow.inner_workflows:
        all_tasks.extend(inner.tasks)
    known_bundle_jobs = {resource_key} | {normalize_task_key(inner.name) for inner in workflow.inner_workflows}
    # ``manual_parameters`` was collected above (before YAML emission) so
    # the broken values are also stripped from the on-disk YAML.
    prereqs = build_prereqs(
        notebooks=all_notebooks,
        tasks=all_tasks,
        known_bundle_jobs=known_bundle_jobs,
        cross_bundle_variables=dict(_cross_bundle_variables),
        manual_parameters=manual_parameters,
    )
    setup_path = output_dir / "SETUP.md"
    setup_path.write_text(render_setup_md(prereqs, bundle_name=effective_name), encoding="utf-8")
    created_files.append(setup_path.resolve())

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
    """CLI entry point for DAB bundle generation."""
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
    _cross_bundle_variables.clear()
    workflows = _load_report(args.report)

    if not workflows:
        print("No translated pipelines found in the report.", file=sys.stderr)
        sys.exit(1)

    all_created: list[Path] = []
    for index, workflow in enumerate(workflows):
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




def _warn(task_key: str, message: str) -> None:
    """Record a translation warning for the current bundle."""
    _bundle_warnings.append(f"- **{task_key}**: {message}")


_DEFAULT_SPARK_VERSION = "15.4.x-scala2.12"
_DEFAULT_NODE_TYPE_ID = "Standard_DS3_v2"


def _infer_bundle_cluster_defaults(workflow: PreparedWorkflow) -> tuple[str, str]:
    """Derive ``spark_version`` and ``node_type_id`` defaults from task clusters.

    Args:
        workflow: The prepared workflow being written.

    Returns:
        ``(spark_version, node_type_id)`` strings.
    """
    from collections import Counter

    spark_versions = [hint["spark_version"] for hint in workflow.cluster_hints if hint.get("spark_version")]
    node_types = [hint["node_type_id"] for hint in workflow.cluster_hints if hint.get("node_type_id")]

    spark_version = Counter(spark_versions).most_common(1)[0][0] if spark_versions else _DEFAULT_SPARK_VERSION
    node_type_id = Counter(node_types).most_common(1)[0][0] if node_types else _DEFAULT_NODE_TYPE_ID
    return spark_version, node_type_id


def _build_databricks_yml(
    bundle_name: str,
    catalog: str,
    schema: str,
    *,
    spark_version: str = _DEFAULT_SPARK_VERSION,
    node_type_id: str = _DEFAULT_NODE_TYPE_ID,
    include_cluster_variables: bool = True,
) -> dict[str, Any]:
    """Builds the root ``databricks.yml`` configuration as a dict.

    Args:
        bundle_name: Name for the bundle.
        catalog: Default target catalog.
        schema: Default target schema.
        spark_version: DBR version for the default job_cluster.  Callers
            typically derive this from :func:`_infer_bundle_cluster_defaults`.
        node_type_id: Instance type for the default job_cluster.
        include_cluster_variables: When True, declares ``spark_version`` and
            ``node_type_id`` variables for the default job_cluster.  Set to
            False when no task in the bundle uses classic compute (every
            generated notebook runs on serverless), so the bundle stays
            free of unused tunables.

    Returns:
        Dict ready for YAML serialization.
    """
    variables: dict[str, Any] = {
        "catalog": {
            "description": "Target catalog",
            "default": catalog,
        },
        "schema": {
            "description": "Target schema",
            "default": schema,
        },
    }
    if include_cluster_variables:
        variables["node_type_id"] = {
            "description": (
                "Instance type for the default job_cluster — override per cloud "
                "(e.g. i3.xlarge on AWS, n1-standard-4 on GCP)."
            ),
            "default": node_type_id,
        }
        variables["spark_version"] = {
            "description": "Databricks Runtime for the default job_cluster.",
            "default": spark_version,
        }
    # Declare a variable for each cross-bundle ExecutePipeline reference so
    # `${var.X_job_id}` resolves and `bundle validate` passes.  Users fill in
    # the numeric job ID per SETUP.md.
    for variable_name, target_pipeline in sorted(_cross_bundle_variables.items()):
        variables[variable_name] = {
            "description": (
                f"Numeric job ID for pipeline '{target_pipeline}' (defined in a sibling bundle). "
                f'Populate via `databricks bundle deploy --var "{variable_name}=<job_id>"` or set '
                "the default here."
            ),
        }
    return {
        "bundle": {
            "name": bundle_name,
        },
        "variables": variables,
        "include": [
            "resources/*.yml",
        ],
        "targets": {
            "dev": {
                "mode": "development",
            },
            "staging": {
                "mode": "production",
            },
            "prod": {
                "mode": "production",
            },
        },
    }


_DEFAULT_JOB_CLUSTER_KEY = "default_cluster"


def _build_default_job_clusters() -> list[dict[str, Any]]:
    """Return a job_clusters stanza that binds notebook tasks to a real cluster."""
    return [
        {
            "job_cluster_key": _DEFAULT_JOB_CLUSTER_KEY,
            "new_cluster": {
                "spark_version": "${var.spark_version}",
                "node_type_id": "${var.node_type_id}",
                "num_workers": 1,
                "data_security_mode": "SINGLE_USER",
            },
        }
    ]


# Patterns that signal a base_parameter value couldn't be evaluated cleanly.
# When any task references an *existing* notebook (absolute workspace path),
# orchestra can't inject the runtime computation, so these end up as manual
# work for the user.
_HYBRID_ADF_FN_RE = re.compile(r"@[a-zA-Z][a-zA-Z0-9]*\(")
_PYTHON_CODE_HINTS = ("dbutils.widgets.get(", "datetime.now(", "datetime.fromisoformat(")


def _value_needs_manual_handling(value: Any) -> bool:
    """Return True when *value* is too dynamic for DAB to substitute at deploy time."""
    if not isinstance(value, str):
        return False
    if _HYBRID_ADF_FN_RE.search(value):
        return True
    return any(hint in value for hint in _PYTHON_CODE_HINTS)


def _extract_manual_parameters_from_existing_notebook_tasks(
    tasks: list[dict[str, Any]],
) -> list[ManualParameter]:
    """Finds base_parameters orchestra couldn't evaluate for existing-notebook tasks."""
    manual_parameters: list[ManualParameter] = []
    for task in _iter_tasks_recursively(tasks):
        notebook_task = task.get("notebook_task") or {}
        notebook_path = notebook_task.get("notebook_path", "")
        base_params = notebook_task.get("base_parameters")
        # Bundle-relative paths (``../src/...``) can have their notebook
        # bodies patched to inline the runtime computation; absolute paths
        # belong to the user's existing notebooks and must be surfaced.
        if not notebook_path.startswith("/") or not isinstance(base_params, dict):
            continue
        keys_to_drop: list[str] = []
        for key, value in base_params.items():
            if not _value_needs_manual_handling(value):
                continue
            manual_parameters.append(
                ManualParameter(
                    task_key=task.get("task_key", ""),
                    widget_name=key,
                    notebook_path=notebook_path,
                    raw_expression=str(value),
                )
            )
            keys_to_drop.append(key)
        for key in keys_to_drop:
            del base_params[key]
        if not base_params:
            notebook_task.pop("base_parameters", None)
    return manual_parameters


def _iter_tasks_recursively(tasks: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """Yields every task in *tasks*, descending into ``for_each_task.task``."""
    for task in tasks:
        yield task
        for_each = task.get("for_each_task") or {}
        inner = for_each.get("task")
        if isinstance(inner, dict):
            yield from _iter_tasks_recursively([inner])


_CLUSTER_BINDING_KEYS = ("existing_cluster_id", "new_cluster", "job_cluster_key")


def _any_task_uses_classic_cluster(tasks: list[dict[str, Any]]) -> bool:
    """Return True if any task (recursively) is bound to a job_cluster_key."""
    return any(task.get("job_cluster_key") for task in _iter_tasks_recursively(tasks))


def _bind_cluster_to_notebook_tasks(tasks: list[dict[str, Any]]) -> None:
    """Attaches the default job_cluster_key to existing-notebook tasks."""
    for task in _iter_tasks_recursively(tasks):
        notebook_task = task.get("notebook_task")
        if notebook_task is None:
            continue
        notebook_path = notebook_task.get("notebook_path", "")
        if notebook_path.startswith("../src/"):
            continue
        if any(key in task for key in _CLUSTER_BINDING_KEYS):
            continue
        task["job_cluster_key"] = _DEFAULT_JOB_CLUSTER_KEY


def _rewrite_post_branch_dependencies(tasks: list[dict[str, Any]]) -> None:
    """Rewrites ``depends_on`` edges that target a condition_task to target its branches.

    Args:
        tasks: Top-level task list, mutated in place.
    """
    condition_keys = {task["task_key"] for task in tasks if "condition_task" in task}
    if not condition_keys:
        return

    direct_outcome_children: dict[str, list[str]] = {key: [] for key in condition_keys}
    for task in tasks:
        for dep in task.get("depends_on") or []:
            if dep.get("task_key") in condition_keys and dep.get("outcome") in ("true", "false"):
                direct_outcome_children[dep["task_key"]].append(task["task_key"])

    def expand_terminals(condition_key: str, seen: set[str]) -> list[str]:
        """Return branch-terminal task keys for a condition, transitively."""
        terminals: list[str] = []
        for child_key in direct_outcome_children.get(condition_key, []):
            if child_key in seen:
                continue
            seen.add(child_key)
            if child_key in condition_keys:
                terminals.extend(expand_terminals(child_key, seen))
            else:
                terminals.append(child_key)
        return terminals

    for task in tasks:
        depends_on = task.get("depends_on") or []
        if not depends_on:
            continue
        rewritten: list[dict[str, Any]] = []
        touched_condition = False
        for dep in depends_on:
            dep_task_key = dep.get("task_key")
            if dep_task_key in condition_keys and "outcome" not in dep:
                replacement_keys = expand_terminals(dep_task_key, set())
                if replacement_keys:
                    rewritten.extend({"task_key": branch_key} for branch_key in replacement_keys)
                    touched_condition = True
                else:
                    rewritten.append(dep)
            else:
                rewritten.append(dep)
        if touched_condition:
            # Drop duplicates — a diamond-shaped join could hit the same
            # terminal via more than one branch.
            seen_keys: set[str] = set()
            deduped: list[dict[str, Any]] = []
            for dep in rewritten:
                key = dep.get("task_key", "")
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped.append(dep)
            task["depends_on"] = deduped
            task.setdefault("run_if", "AT_LEAST_ONE_SUCCESS")


_TASK_VALUE_REF = re.compile(r"\{\{tasks\.([^.]+)\.values\.[^}]+\}\}")


def _strip_dangling_task_value_refs(tasks: list[dict[str, Any]], all_task_keys: set[str]) -> None:
    """Replaces ``{{tasks.X.values.Y}}`` refs whose ``X`` is not in the bundle.

    Args:
        tasks: Top-level tasks for one job (mutated in place).
        all_task_keys: Task keys that do exist in this job (including those
            inside ``for_each_task.task`` bodies).
    """

    def visit(task: dict[str, Any]) -> None:
        notebook_task = task.get("notebook_task") or {}
        base_parameters = notebook_task.get("base_parameters") or {}
        for widget_name, value in list(base_parameters.items()):
            if not isinstance(value, str):
                continue
            match = _TASK_VALUE_REF.search(value)
            if match and match.group(1) not in all_task_keys:
                base_parameters[widget_name] = ""
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            visit(for_each["task"])

    for task in tasks:
        visit(task)


def _collect_all_task_keys(tasks: list[dict[str, Any]]) -> set[str]:
    """Collects every task_key reachable from the job's top-level task list."""
    keys: set[str] = set()
    for task in tasks:
        keys.add(task.get("task_key", ""))
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            keys.update(_collect_all_task_keys([for_each["task"]]))
    keys.discard("")
    return keys


def _augment_base_parameters(tasks: list[dict[str, Any]], notebooks: list[DabNotebook]) -> None:
    """Ensure every widget a notebook reads is declared in its base_parameters.

    Args:
        tasks: Top-level task dicts (mutated in place).
        notebooks: Generated notebooks to scan.
    """
    notebook_by_relpath = {notebook.relative_path: notebook for notebook in notebooks}

    def visit(task: dict[str, Any]) -> None:
        notebook_task = task.get("notebook_task")
        if notebook_task:
            notebook_path = notebook_task.get("notebook_path", "")
            relative = notebook_path[len("../src/") :] if notebook_path.startswith("../src/") else ""
            notebook = notebook_by_relpath.get(relative)
            if notebook:
                widgets = set(_WIDGET_REFERENCE.findall(notebook.content))
                base_parameters = notebook_task.setdefault("base_parameters", {})
                for widget_name in sorted(widgets):
                    base_parameters.setdefault(widget_name, "")
        for_each = task.get("for_each_task")
        if for_each and isinstance(for_each.get("task"), dict):
            visit(for_each["task"])

    for task in tasks:
        visit(task)


def _build_job_resource(
    workflow: PreparedWorkflow,
    resource_key: str,
    *,
    attach_clusters: bool = True,
    extra_notebooks_for_augment: list[DabNotebook] | None = None,
) -> dict[str, Any]:
    """Builds a job resource dict for a single workflow.

    Args:
        workflow: The prepared workflow to serialize.
        resource_key: The sanitised resource key for this job.
        attach_clusters: When ``True`` (default) emits a ``job_clusters`` block
            and binds every notebook task to it.  Set to ``False`` for inner
            jobs that are invoked via ``run_job_task`` from another bundle
            job — they inherit compute from the caller.

    Returns:
        Dict ready for YAML serialization.
    """
    _rewrite_post_branch_dependencies(workflow.tasks)
    # For inner jobs (invoked via run_job_task), notebooks live in the parent
    # workflow's notebooks list — pass them in so widget auto-augment can
    # still find the bound notebook and populate base_parameters.
    augment_scope = list(workflow.notebooks) + list(extra_notebooks_for_augment or [])
    _augment_base_parameters(workflow.tasks, augment_scope)
    # Task values don't cross ``run_job_task`` boundaries; any such
    # reference in this job resolves to an empty string at runtime.  Emit
    # the empty string now so SETUP.md §4 flags it.
    _strip_dangling_task_value_refs(workflow.tasks, _collect_all_task_keys(workflow.tasks))

    job_def: dict[str, Any] = {
        "name": workflow.name,
        "tasks": workflow.tasks,
    }

    if attach_clusters:
        _bind_cluster_to_notebook_tasks(workflow.tasks)
        # Only emit the ``job_clusters`` block when at least one task is
        # actually bound to it.  When every task runs on serverless (the
        # generated-notebook case), the job stays cluster-free and inherits
        # the workspace's serverless defaults.
        if _any_task_uses_classic_cluster(workflow.tasks):
            job_def["job_clusters"] = _build_default_job_clusters()

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

    Args:
        params: Raw base_parameters dict from the IR.
        task_key: Task key for warning attribution.

    Returns:
        Dict with all values resolved to strings.
    """
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




def _load_report(report_path: Path) -> list[PreparedWorkflow]:
    """Loads a translation report and reconstruct PreparedWorkflow objects.

    Args:
        report_path: Path to the translation report JSON file.

    Returns:
        List of PreparedWorkflow objects, one per pipeline.
    """
    with open(report_path, encoding="utf-8") as report_file:
        report = json.load(report_file)

    workflows: list[PreparedWorkflow] = []

    if "tasks" in report and "name" in report:
        workflow = _pipeline_dict_to_workflow(report)
        workflows.append(workflow)
        return workflows

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
    """Converts a serialized pipeline IR dict to a PreparedWorkflow.

    Args:
        pipeline_dict: Dict loaded from a pipeline IR JSON file.

    Returns:
        A PreparedWorkflow with tasks extracted from the IR.
    """
    tasks: list[dict[str, Any]] = []
    notebooks: list[DabNotebook] = []
    inner_workflows: list[PreparedWorkflow] = []
    cluster_hints: list[dict[str, Any]] = []
    task_key_remap: dict[str, str] = {}

    for task_ir in pipeline_dict.get("tasks", []):
        result = _task_ir_to_dab(task_ir, task_key_remap=task_key_remap)
        tasks.append(result["task"])
        tasks.extend(result.get("extra_tasks", []))
        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])
        cluster = task_ir.get("cluster")
        if cluster:
            cluster_hints.append(dict(cluster))

    # Apply Switch-case renames: any task whose ``depends_on`` referenced
    # a Switch's bare task_key is rewired to the renamed first case.
    if task_key_remap:
        for task in tasks:
            for dep in task.get("depends_on", []) or []:
                old = dep.get("task_key")
                if old in task_key_remap:
                    dep["task_key"] = task_key_remap[old]

    # Carry pipeline-level parameters through to the job definition.
    # Normalise ADF expressions in defaults so `@pipeline().RunId` etc. become
    # DAB dynamic value references.
    parameters: list[dict[str, Any]] = []
    for param in pipeline_dict.get("parameters") or []:
        param_entry: dict[str, Any] = {"name": param["name"]}
        if "default" in param and param["default"] is not None:
            param_entry["default"] = _normalize_value(str(param["default"]))
        parameters.append(param_entry)

    # The CLI reload path doesn't carry the IR-level SecretInstructions, so
    # mine them back out of the notebook bodies we just generated.  That way
    # the setup notebook generator and SETUP.md writer both see the same
    # set of scopes/keys the runtime code will demand.
    secret_refs = scan_notebooks_for_secrets(notebooks)
    for inner in inner_workflows:
        for scope_name, keys in scan_notebooks_for_secrets(inner.notebooks).items():
            secret_refs.setdefault(scope_name, set()).update(keys)
    secrets: list[SecretInstruction] = []
    for scope_name in sorted(secret_refs):
        for key in sorted(secret_refs[scope_name]):
            secrets.append(
                SecretInstruction(
                    scope=scope_name,
                    key=key,
                    value_source=(
                        f"Credential for scope '{scope_name}', key '{key}' — referenced by generated notebooks."
                    ),
                )
            )

    # Reconstitute SetupTasks from the IR.  The CLI reload path doesn't
    # carry the in-process preparer's setup_tasks list, so we mine the
    # serialised CopyActivity sink/source properties for ``volume_*`` hints
    # and emit one SetupTask per unique external location.  Without this,
    # bundles produced via ``dab_writer.py --report ...`` would silently
    # drop the volume-creation step.
    setup_tasks = _collect_setup_tasks_from_ir(pipeline_dict)

    return PreparedWorkflow(
        name=pipeline_dict.get("name", "unknown"),
        tasks=tasks,
        notebooks=notebooks,
        secrets=secrets,
        setup_tasks=setup_tasks,
        inner_workflows=inner_workflows,
        parameters=parameters,
        cluster_hints=cluster_hints,
    )


def _collect_setup_tasks_from_ir(pipeline_dict: dict[str, Any]) -> list[SetupTask]:
    """Walks a pipeline IR dict and emit setup tasks for sink-side UC volumes."""
    setup_tasks: list[SetupTask] = []
    seen_volumes: set[str] = set()

    def _walk(tasks_iterable):
        for task_ir in tasks_iterable:
            if task_ir.get("type") == "CopyActivity":
                sink_props = task_ir.get("sink_properties") or {}
                volume_name = sink_props.get("volume_name")
                external_location = sink_props.get("volume_external_location")
                location_type = sink_props.get("volume_location_type") or ""
                if volume_name and external_location and volume_name not in seen_volumes:
                    seen_volumes.add(volume_name)
                    setup_tasks.append(
                        SetupTask(
                            type="volume",
                            config={
                                "volume_name": volume_name,
                                "volume_type": "EXTERNAL",
                                "location": external_location,
                                "location_type": location_type,
                                "storage_account": sink_props.get("volume_storage_account"),
                            },
                        )
                    )
            for inner in task_ir.get("inner_activities") or []:
                _walk([inner])
            for case in task_ir.get("cases") or []:
                _walk(case.get("activities") or [])
            _walk(task_ir.get("default_activities") or [])
            _walk(task_ir.get("if_true_activities") or [])
            _walk(task_ir.get("if_false_activities") or [])

    _walk(pipeline_dict.get("tasks", []))
    return setup_tasks


def _reconstruct_ir(task_ir: dict[str, Any]) -> Any:
    """Reconstruct a typed IR activity object from a serialized dict.

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
            sink_dataset_type=task_ir.get("sink_dataset_type"),
            sink_format=task_ir.get("sink_format"),
            sink_resolved_path=task_ir.get("sink_resolved_path"),
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
            required_parameters=task_ir.get("required_parameters", {}),
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
            required_parameters=task_ir.get("required_parameters", {}),
        )
    return None


# Notebook content generators


def _motif_notebook(
    *,
    task_key: str,
    activity_name: str,
    motif_id: str,
    databricks_replacement: str,
    matched_activity_names: list[str],
    source_type_hint: str,
    confidence_notes: list[str],
    motif_config: dict[str, Any] | None = None,
) -> str:
    """Generates a notebook for a collapsed motif activity."""
    matched_list = "\n".join(f"# MAGIC - `{name}`" for name in matched_activity_names)
    notes_list = "\n".join(f"# MAGIC - {note}" for note in confidence_notes) if confidence_notes else "# MAGIC   (none)"

    source_line = f"# MAGIC **Source type**: `{source_type_hint}`" if source_type_hint else ""

    lines = [
        "# Databricks notebook source",
        "# MAGIC %md",
        f"# MAGIC # Motif: {activity_name}",
        "# MAGIC",
        f"# MAGIC **Pattern**: `{motif_id}`",
        f"# MAGIC **Databricks replacement**: `{databricks_replacement}`",
    ]
    if source_line:
        lines.append(source_line)
    lines.extend(
        [
            "# MAGIC",
            "# MAGIC ## Collapsed ADF Activities",
            "# MAGIC",
            matched_list,
            "# MAGIC",
            "# MAGIC ## Detection Notes",
            "# MAGIC",
            notes_list,
            "# MAGIC",
            "# MAGIC *Auto-generated by Orchestra motif collapser.*",
            "",
            "# COMMAND ----------",
            "",
        ]
    )

    if databricks_replacement == "auto_loader":
        lines.extend(
            [
                "# Auto Loader ingestion — replaces Lookup/Copy/StoredProcedure watermark chain",
                "source_path = dbutils.widgets.get('source_path')",
                "target_table = dbutils.widgets.get('target_table')",
                f"checkpoint_path = '/tmp/checkpoints/{task_key}'",
                "",
                "df = (",
                '    spark.readStream.format("cloudFiles")',
                '    .option("cloudFiles.format", "parquet")',
                '    .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")',
                "    .load(source_path)",
                ")",
                "",
                "(",
                '    df.writeStream.format("delta")',
                '    .option("checkpointLocation", checkpoint_path)',
                '    .option("mergeSchema", "true")',
                "    .outputMode('append')",
                "    .trigger(availableNow=True)",
                "    .toTable(target_table)",
                ")",
            ]
        )
    elif databricks_replacement == "dlt_apply_changes":
        lines.extend(
            [
                "# DLT APPLY CHANGES — replaces Copy/DataFlow SCD or CDC chain",
                "# This motif is best implemented as a DLT pipeline definition.",
                "# See: https://docs.databricks.com/en/delta-live-tables/cdc.html",
                "",
                "# import dlt",
                "# @dlt.table",
                "# def target_table():",
                "#     return spark.readStream.table('staging_table')",
                "#",
                "# dlt.apply_changes(",
                "#     target='target_table',",
                "#     source='staging_table',",
                "#     keys=['id'],",
                "#     sequence_by='updated_at',",
                "# )",
                "",
                f"raise NotImplementedError('Motif {motif_id}: implement as DLT pipeline')",
            ]
        )
    elif databricks_replacement == "for_each_ingestion":
        motif_config = motif_config or {}
        lookup_query = motif_config.get("lookup_query", "")
        lookup_scope = motif_config.get("lookup_scope") or task_key
        copy_scope = motif_config.get("copy_scope") or task_key
        sink_table_pattern = motif_config.get("sink_table") or "raw.{schema_name}_{table_name}"
        lines.extend(
            [
                "# Parameterised bulk ingestion — replaces the collapsed Lookup/ForEach/Copy chain.",
                "# The driving list of items is fetched here (the original ADF Lookup's query),",
                "# then each row's fields parameterise a JDBC read into Delta.",
                "import json",
                "",
                "# Credentials used for both the control-table lookup and the per-row reads.",
                f"lookup_jdbc_url = dbutils.secrets.get(scope='{lookup_scope}', key='jdbc-url')",
                f"lookup_jdbc_user = dbutils.secrets.get(scope='{lookup_scope}', key='jdbc-user')",
                f"lookup_jdbc_password = dbutils.secrets.get(scope='{lookup_scope}', key='jdbc-password')",
                "",
                "# Override via `items` widget if the caller already has the list on hand",
                "# (useful for dev / single-row replays); otherwise fetch it from the source.",
                "items_override = dbutils.widgets.get('items')",
                "if items_override:",
                "    items = json.loads(items_override)",
                "else:",
                f"    control_query = {lookup_query!r}",
                "    control_df = (",
                "        spark.read.format('jdbc')",
                "        .option('url', lookup_jdbc_url)",
                "        .option('user', lookup_jdbc_user)",
                "        .option('password', lookup_jdbc_password)",
                "        .option('query', control_query)",
                "        .load()",
                "    )",
                "    items = [row.asDict() for row in control_df.collect()]",
                "",
                f"copy_jdbc_url = dbutils.secrets.get(scope='{copy_scope}', key='jdbc-url')",
                f"copy_jdbc_user = dbutils.secrets.get(scope='{copy_scope}', key='jdbc-user')",
                f"copy_jdbc_password = dbutils.secrets.get(scope='{copy_scope}', key='jdbc-password')",
                "",
                "for item in items:",
                "    table_name = item.get('table_name') or item.get('name') or 'UNKNOWN_TABLE'",
                "    schema_name = item.get('schema_name', 'dbo')",
                f"    target = {sink_table_pattern!r}.format(schema_name=schema_name, table_name=table_name)",
                "    query = f'SELECT * FROM {schema_name}.{table_name}'",
                "    (",
                "        spark.read.format('jdbc')",
                "        .option('url', copy_jdbc_url)",
                "        .option('user', copy_jdbc_user)",
                "        .option('password', copy_jdbc_password)",
                "        .option('query', query)",
                "        .load()",
                "        .write.format('delta')",
                "        .mode('overwrite')",
                "        .option('overwriteSchema', 'true')",
                "        .saveAsTable(target)",
                "    )",
                "",
                "dbutils.notebook.exit(json.dumps({'ingested_tables': len(items)}))",
            ]
        )
    elif databricks_replacement == "python_rest_ingestion":
        lines.extend(
            [
                "# REST API pagination — replaces WebActivity/Until/SetVariable chain",
                "import json",
                "import requests",
                "",
                "base_url = dbutils.widgets.get('api_url')",
                "auth_token = dbutils.secrets.get(scope='rest_api', key='token')",
                "headers = {'Authorization': f'Bearer {auth_token}'}",
                "",
                "all_records = []",
                "next_url = base_url",
                "",
                "while next_url:",
                "    response = requests.get(next_url, headers=headers, timeout=60)",
                "    response.raise_for_status()",
                "    data = response.json()",
                "    records = data.get('value', data.get('data', []))",
                "    all_records.extend(records)",
                "    next_url = data.get('nextLink') or data.get('@odata.nextLink')",
                "",
                "df = spark.createDataFrame(all_records)",
                "target_table = dbutils.widgets.get('target_table')",
                "df.write.format('delta').mode('overwrite').saveAsTable(target_table)",
                "print(f'Ingested {len(all_records)} records')",
            ]
        )
    elif databricks_replacement == "auto_loader_file_notification":
        lines.extend(
            [
                "# Auto Loader with file notification — replaces GetMetadata/ForEach/Copy/Delete chain",
                "source_path = dbutils.widgets.get('source_path')",
                "target_table = dbutils.widgets.get('target_table')",
                f"checkpoint_path = '/tmp/checkpoints/{task_key}'",
                "",
                "df = (",
                '    spark.readStream.format("cloudFiles")',
                '    .option("cloudFiles.format", "parquet")',
                '    .option("cloudFiles.useNotifications", "true")',
                '    .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")',
                "    .load(source_path)",
                ")",
                "",
                "(",
                '    df.writeStream.format("delta")',
                '    .option("checkpointLocation", checkpoint_path)',
                "    .outputMode('append')",
                "    .trigger(availableNow=True)",
                "    .toTable(target_table)",
                ")",
            ]
        )
    else:
        lines.extend(
            [
                f"# TODO: Implement Databricks-native replacement for motif '{motif_id}'",
                f"# Strategy: {databricks_replacement}",
                f"raise NotImplementedError('Motif {motif_id}: implement {databricks_replacement}')",
            ]
        )

    lines.append("")
    return "\n".join(lines)


def _placeholder_notebook(task_key: str, activity_name: str, activity_type: str, original_path: str = "") -> str:
    """Generates placeholder notebook content for the JSON reload path.

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
    """Generates a notebook that performs an HTTP request for a WebActivity.

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

    normalised_url = _normalize_value(url)

    body_code = ""
    if body_raw:
        if isinstance(body_raw, dict) and body_raw.get("type") == "Expression":
            expression = body_raw.get("value", "")
            body_code = (
                f"# ADF expression: {expression}\n# TODO: Translate the body expression to Python\npayload = {{}}"
            )
        elif isinstance(body_raw, dict):
            body_code = f"payload = {json.dumps(body_raw, indent=4)}"
        elif isinstance(body_raw, str):
            body_code = f'payload = "{body_raw}"'
        else:
            body_code = f"payload = {body_raw!r}"
    else:
        body_code = "payload = None"

    headers_code = "headers = None"
    if headers_raw and isinstance(headers_raw, dict):
        headers_code = f"headers = {json.dumps(headers_raw, indent=4)}"

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
    """Tries to download a notebook from the workspace; fall back to a placeholder.

    Args:
        workspace_path: Original workspace path from the ADF definition.
        task_key: Sanitised task key (used in the placeholder filename).
        activity_name: ADF activity display name.
        task_type_name: IR activity type name.

    Returns:
        Notebook source content as a string.
    """
    if workspace_path:
        content = download_notebook(workspace_path)
        if content is not None:
            return content

    return _placeholder_notebook(task_key, activity_name, task_type_name, workspace_path)




def _task_ir_to_dab(
    task_ir: dict[str, Any],
    *,
    task_key_remap: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Converts a single serialized task IR dict to a DAB task dict.

    Args:
        task_ir: Dict for a single task from the serialized IR JSON.
        task_key_remap: Mutable mapping of original task_key to renamed
            task_key.  Currently only ``_handle_switch`` writes into it
            (when the first switch case is renamed to ``__case_<value>``);
            the caller applies the remap to all tasks at the end of the
            pipeline conversion to preserve depends_on edges.

    Returns:
        Dict with ``task`` (DAB task dict), ``extra_tasks`` (siblings),
        ``notebooks`` (list of DabNotebook), and ``inner_workflows``
        (list of PreparedWorkflow).
    """
    task_key = task_ir.get("task_key", "")
    activity_name = task_ir.get("name", task_key)
    task: dict[str, Any] = {"task_key": task_key}
    task_type_name = task_ir.get("type", "")
    extra_tasks: list[dict[str, Any]] = []
    notebooks: list[DabNotebook] = []
    inner_workflows: list[PreparedWorkflow] = []

    if task_ir.get("depends_on"):
        task["depends_on"] = [{"task_key": dep["task_key"]} for dep in task_ir["depends_on"]]
        run_if = run_if_from_adf_outcomes([dep.get("outcome") for dep in task_ir["depends_on"]])
        if run_if:
            task["run_if"] = run_if
    if task_ir.get("timeout_seconds"):
        task["timeout_seconds"] = task_ir["timeout_seconds"]
    if task_ir.get("max_retries"):
        task["max_retries"] = task_ir["max_retries"]
    if task_ir.get("min_retry_interval_millis"):
        task["min_retry_interval_millis"] = task_ir["min_retry_interval_millis"]

    if task_type_name == "ForEachActivity":
        _handle_for_each(task, task_ir, task_key, notebooks, inner_workflows)

    elif task_type_name == "SwitchActivity":
        _handle_switch(task, task_ir, task_key, notebooks, inner_workflows, extra_tasks, task_key_remap)

    elif task_type_name == "IfConditionActivity":
        _handle_if_condition(task, task_ir, task_key, notebooks, inner_workflows, extra_tasks)

    elif task_type_name == "NotebookActivity":
        original_path = task_ir.get("notebook_path", "")

        # If the ADF activity already references a real workspace notebook
        # (an absolute path like /Shared/team/foo), bind the task to that
        # path directly — the existing notebook is the source of truth and
        # bundling a copy creates divergence.
        if original_path.startswith("/"):
            task["notebook_task"] = {"notebook_path": original_path}
            if task_ir.get("base_parameters"):
                task["notebook_task"]["base_parameters"] = _normalize_base_parameters(
                    task_ir["base_parameters"],
                    task_key=task_key,
                )
        else:
            notebook_relative_path = f"notebooks/{notebook_filename(task_key, activity_name)}"
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
                resolved_params.append(_normalize_value(param))
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
            # Cross-bundle references can't resolve via ${resources.jobs.X.id} —
            # the referenced job lives in a sibling bundle.  Emit a bundle
            # variable instead so `bundle validate` passes; the SETUP writer
            # picks this up and tells the user to populate it with a numeric
            # job ID.
            ref_key = normalize_task_key(task_ir["pipeline_name"])
            variable_name = f"{ref_key}_job_id"
            run_job["job_id"] = f"${{var.{variable_name}}}"
            _cross_bundle_variables.setdefault(variable_name, task_ir["pipeline_name"])
        elif task_ir.get("job_name"):
            run_job["job_id"] = f"${{resources.jobs.{task_ir['job_name']}.id}}"
        if task_ir.get("parameters"):
            job_params: dict[str, str] = {}
            for key, value in task_ir["parameters"].items():
                normalized = _normalize_value(value)
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
        notebook_relative_path = f"notebooks/{notebook_filename(task_key, activity_name)}"
        # Generated notebooks (Lookup, SetVariable, Wait, Filter, WebActivity,
        # Delete, AppendVariable, Copy) inherit the workspace's default
        # serverless config -- they install no Python libraries, so we don't
        # bind them to an ``environment_key`` or emit an ``environments`` block.
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
            sink_props = task_ir.get("sink_properties") or {}
            volume_name = sink_props.get("volume_name")
            if volume_name:
                # Volume root is DAB-substituted in the YAML; the notebook
                # reads it as ``output_path_root`` and joins the resolved
                # relative path on top.
                base_parameters["output_path_root"] = f"/Volumes/${{var.catalog}}/${{var.schema}}/{volume_name}"
            if base_parameters:
                task["notebook_task"]["base_parameters"] = base_parameters
        elif task_type_name == "WebActivity":
            task["notebook_task"]["base_parameters"] = {
                "url": _normalize_value(task_ir.get("url", "")),
                "method": task_ir.get("method", "GET"),
            }
        elif task_type_name == "SetVariableActivity":
            set_variable_params: dict[str, str] = {"variable_name": task_ir.get("variable_name", "")}
            if task_ir.get("value_kind") in ("literal", "dab_ref"):
                set_variable_params["value"] = _normalize_value(task_ir.get("variable_value", ""))
            for widget_name, dab_ref in (task_ir.get("required_parameters") or {}).items():
                set_variable_params.setdefault(widget_name, dab_ref)
            task["notebook_task"]["base_parameters"] = set_variable_params
        elif task_type_name == "AppendVariableActivity":
            append_variable_params: dict[str, str] = {"variable_name": task_ir.get("variable_name", "")}
            if task_ir.get("value_kind") in ("literal", "dab_ref"):
                append_variable_params["value"] = _normalize_value(task_ir.get("append_value", ""))
            for widget_name, dab_ref in (task_ir.get("required_parameters") or {}).items():
                append_variable_params.setdefault(widget_name, dab_ref)
            task["notebook_task"]["base_parameters"] = append_variable_params

        notebooks.append(
            DabNotebook(
                relative_path=notebook_relative_path,
                content=content,
            )
        )

    elif task_type_name == "MotifActivity":
        notebook_relative_path = f"notebooks/{task_key}.py"
        task["notebook_task"] = {"notebook_path": f"../src/{notebook_relative_path}"}
        motif_id = task_ir.get("motif_id", "unknown")
        databricks_replacement = task_ir.get("databricks_replacement", "notebook")
        matched_activity_names = task_ir.get("matched_activity_names", [])
        source_type_hint = task_ir.get("source_type_hint", "")
        confidence_notes = task_ir.get("confidence_notes", [])
        motif_config = task_ir.get("motif_config") or {}

        content = _motif_notebook(
            task_key=task_key,
            activity_name=activity_name,
            motif_id=motif_id,
            databricks_replacement=databricks_replacement,
            matched_activity_names=matched_activity_names,
            source_type_hint=source_type_hint,
            confidence_notes=confidence_notes,
            motif_config=motif_config,
        )
        notebooks.append(DabNotebook(relative_path=notebook_relative_path, content=content))

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

    return {
        "task": task,
        "extra_tasks": extra_tasks,
        "notebooks": notebooks,
        "inner_workflows": inner_workflows,
    }




def inject_outcome_dependency(tasks: list[dict[str, Any]], condition_key: str, outcome: str) -> None:
    """Gate root tasks in a branch on the condition's outcome.

    Args:
        tasks: Tasks in one branch (mutated in place).
        condition_key: Task key of the enclosing condition task.
        outcome: ``"true"`` or ``"false"``.
    """
    branch_keys = {task.get("task_key") for task in tasks}
    for task in tasks:
        deps = task.get("depends_on") or []
        refers_to_branch_sibling = any(dep.get("task_key") in branch_keys for dep in deps)
        if not deps or not refers_to_branch_sibling:
            task["depends_on"] = [{"task_key": condition_key, "outcome": outcome}]


def _collect_branch_tasks(
    child_irs: list[dict[str, Any]],
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
) -> list[dict[str, Any]]:
    """Prepares a branch of child IR dicts into a flat list of DAB task dicts.

    Args:
        child_irs: Serialized IR dicts for one branch.
        notebooks: Accumulator for generated notebooks (mutated).
        inner_workflows: Accumulator for inner job workflows (mutated).

    Returns:
        Flat list of DAB task dicts for the branch.
    """
    branch_tasks: list[dict[str, Any]] = []
    for child_ir in child_irs:
        result = _task_ir_to_dab(child_ir)
        branch_tasks.append(result["task"])
        branch_tasks.extend(result.get("extra_tasks", []))
        notebooks.extend(result["notebooks"])
        inner_workflows.extend(result["inner_workflows"])
    return branch_tasks


def _handle_if_condition(
    task: dict[str, Any],
    task_ir: dict[str, Any],
    task_key: str,
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
    extra_tasks: list[dict[str, Any]],
) -> None:
    """Builds a condition_task and flatten both branches into sibling tasks.

    Args:
        task: The DAB task dict being built (mutated in place).
        task_ir: Serialized IfConditionActivity dict from the IR JSON.
        task_key: Task key for the IfCondition activity.
        notebooks: Accumulator for generated notebooks.
        inner_workflows: Accumulator for inner job workflows.
        extra_tasks: Accumulator for the flattened branch tasks (mutated).
    """
    task["condition_task"] = {
        "op": task_ir.get("op", "EQUAL_TO"),
        "left": task_ir.get("left", ""),
        "right": task_ir.get("right", ""),
    }

    if_true_tasks = _collect_branch_tasks(task_ir.get("if_true_activities", []), notebooks, inner_workflows)
    inject_outcome_dependency(if_true_tasks, task_key, "true")
    extra_tasks.extend(if_true_tasks)

    if_false_tasks = _collect_branch_tasks(task_ir.get("if_false_activities", []), notebooks, inner_workflows)
    inject_outcome_dependency(if_false_tasks, task_key, "false")
    extra_tasks.extend(if_false_tasks)


def _handle_switch(
    task: dict[str, Any],
    task_ir: dict[str, Any],
    task_key: str,
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
    extra_tasks: list[dict[str, Any]],
    task_key_remap: dict[str, str] | None = None,
) -> None:
    """Builds a chain of condition_tasks from a SwitchActivity IR dict.

    Args:
        task: The DAB task dict being built (mutated in place) — becomes the
            first case's condition task.
        task_ir: Serialized SwitchActivity dict from the IR JSON.
        task_key: Task key for the Switch activity.
        notebooks: Accumulator for generated notebooks.
        inner_workflows: Accumulator for inner job workflows.
        extra_tasks: Accumulator for all subsequent condition tasks and
            branch bodies (mutated).
    """
    on_expression = task_ir.get("on_expression", "")
    cases = task_ir.get("cases", [])
    default_activity_dicts = task_ir.get("default_activities", [])

    if not cases:
        task["condition_task"] = {"op": "EQUAL_TO", "left": "true", "right": "true"}
        default_tasks = _collect_branch_tasks(default_activity_dicts, notebooks, inner_workflows)
        inject_outcome_dependency(default_tasks, task_key, "true")
        extra_tasks.extend(default_tasks)
        return

    # Every case (including the first) is named ``<switch>_case_<value>``.
    # The first case keeps the Switch's original ``depends_on`` edges, but
    # the task_key is renamed so the rendered job graph reads cleanly.  Any
    # downstream task whose ``depends_on`` referenced the bare Switch key
    # is rewritten by the caller after all tasks are converted (see
    # ``task_key_remap`` in ``_pipeline_dict_to_workflow``).
    case_keys: list[str] = []
    for index, case in enumerate(cases):
        case_value = case.get("value", "")
        is_first = index == 0
        case_key = f"{task_key}_case_{_sanitize_case_key(case_value)}"
        case_keys.append(case_key)

        if is_first:
            task["task_key"] = case_key
            task["condition_task"] = {"op": "EQUAL_TO", "left": on_expression, "right": case_value}
        else:
            extra_tasks.append(
                {
                    "task_key": case_key,
                    "depends_on": [{"task_key": case_keys[index - 1], "outcome": "false"}],
                    "condition_task": {"op": "EQUAL_TO", "left": on_expression, "right": case_value},
                }
            )

        branch_tasks = _collect_branch_tasks(case.get("activities", []), notebooks, inner_workflows)
        inject_outcome_dependency(branch_tasks, case_key, "true")
        extra_tasks.extend(branch_tasks)

    # Record the rename so downstream tasks that referenced the bare
    # Switch task_key are rewired to the new first-case key.
    if task_key_remap is not None and case_keys:
        task_key_remap[task_key] = case_keys[0]

    default_tasks = _collect_branch_tasks(default_activity_dicts, notebooks, inner_workflows)
    if default_tasks:
        inject_outcome_dependency(default_tasks, case_keys[-1], "false")
        extra_tasks.extend(default_tasks)


def _sanitize_case_key(value: str) -> str:
    """Sanitizes a case value for use as a task key suffix."""
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "empty"


def _handle_for_each(
    task: dict[str, Any],
    task_ir: dict[str, Any],
    task_key: str,
    notebooks: list[DabNotebook],
    inner_workflows: list[PreparedWorkflow],
) -> None:
    """Builds a for_each_task from a serialized ForEachActivity IR dict.

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
        result = _task_ir_to_dab(inner_activity_dicts[0])
        inner_task = result["task"]
        inner_key = inner_task.get("task_key", task_key)
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
        inner_job_name = f"{task_key}_inner_tasks"
        inner_job_key = normalize_task_key(inner_job_name)

        inner_tasks: list[dict[str, Any]] = []
        for inner_ir in inner_activity_dicts:
            result = _task_ir_to_dab(inner_ir)
            inner_tasks.append(result["task"])
            notebooks.extend(result["notebooks"])
            inner_workflows.extend(result["inner_workflows"])

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
        task["for_each_task"] = {
            "inputs": items_expression,
            "task": {"task_key": f"{task_key}_noop"},
            "concurrency": concurrency,
        }


if __name__ == "__main__":
    main()
