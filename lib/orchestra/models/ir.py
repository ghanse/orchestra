"""Translation IR -- intermediate representation after translation.

This module defines the typed intermediate representation produced by translating
ADF AST nodes into Databricks-oriented structures.  The IR is consumed by the
preparer and bundler stages to emit DAB bundles.

Ported from wkmigrate with modifications for the orchestra plugin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, TypeAlias

# ---------------------------------------------------------------------------
# Expression result
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class ExpressionResult:
    """Result of resolving an ADF expression.

    kind:
      - "literal": a plain string/number value for base_parameters
      - "dab_ref": a DAB dynamic value reference like {{job.run_id}}
      - "notebook_code": Python code that must go in the notebook body
    value: the resolved value (literal, DAB ref template, or Python code)
    imports: Python import statements needed (only for notebook_code)
    """

    kind: str  # "literal", "dab_ref", "notebook_code"
    value: str
    imports: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class Dependency:
    """Dependency on an upstream task.

    Attributes:
        task_key: Task key of the upstream activity.
        outcome: Required outcome (e.g. ``"Succeeded"``) for this edge.
    """

    task_key: str
    outcome: str | None = None


# ---------------------------------------------------------------------------
# Activity hierarchy
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class Activity:
    """Base class for all translated pipeline activities.

    Attributes:
        name: Logical activity name from ADF.
        task_key: Unique task key within the workflow.
        description: Human-readable description.
        timeout_seconds: Maximum execution time in seconds.
        max_retries: Retry limit on failure.
        min_retry_interval_millis: Minimum delay between retries (ms).
        depends_on: Upstream task dependencies.
        cluster: Cluster configuration for the task, if any.
    """

    name: str
    task_key: str
    description: str | None = None
    timeout_seconds: int | None = None
    max_retries: int | None = None
    min_retry_interval_millis: int | None = None
    depends_on: list[Dependency] | None = None
    cluster: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class NotebookActivity(Activity):
    """Databricks notebook activity.

    Attributes:
        notebook_path: Workspace path to the notebook.
        base_parameters: Parameters passed to the notebook at runtime.
        linked_service_definition: Raw linked-service dictionary for cluster config.
    """

    notebook_path: str
    base_parameters: dict[str, str] | None = None
    linked_service_definition: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class CopyActivity(Activity):
    """Copy data activity.

    Attributes:
        source_type: Source dataset type string.
        sink_type: Sink dataset type string.
        source_properties: Parsed source format/connection options.
        sink_properties: Parsed sink format/connection options.
        column_mapping: Column-level source-to-sink mappings.
    """

    source_type: str | None = None
    sink_type: str | None = None
    source_properties: dict[str, Any] | None = None
    sink_properties: dict[str, Any] | None = None
    column_mapping: list[dict[str, str]] | None = None


@dataclass(slots=True, kw_only=True)
class ForEachActivity(Activity):
    """ForEach loop activity.

    Attributes:
        items_expression: ADF expression driving the iteration.
        child_activity: Activity to execute per item.
        concurrency: Maximum parallel iterations.
    """

    items_expression: str
    child_activity: Activity
    concurrency: int | None = None


@dataclass(slots=True, kw_only=True)
class IfConditionActivity(Activity):
    """If condition branching activity.

    Attributes:
        op: Comparison operator name.
        left: Left-hand operand expression.
        right: Right-hand operand expression.
        if_true_activities: Activities for the true branch.
        if_false_activities: Activities for the false branch.
    """

    op: str
    left: str
    right: str
    if_true_activities: list[Activity] = field(default_factory=list)
    if_false_activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class SetVariableActivity(Activity):
    """Set variable activity.

    Attributes:
        variable_name: Name of the variable being set.
        variable_value: Expression string that evaluates to the value.
        value_kind: Kind of the resolved expression ("literal", "dab_ref", "notebook_code").
        notebook_code: Python code for notebook_code kind values.
        notebook_imports: Import statements needed for notebook_code.
    """

    variable_name: str
    variable_value: str
    value_kind: str = "literal"  # "literal", "dab_ref", "notebook_code"
    notebook_code: str | None = None
    notebook_imports: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class LookupActivity(Activity):
    """Lookup activity.

    Attributes:
        source_type: Type of the lookup dataset.
        source_properties: Parsed source format/connection options.
        first_row_only: When True, only the first row is returned.
        source_query: Optional SQL query or stored-procedure call.
    """

    source_type: str | None = None
    source_properties: dict[str, Any] | None = None
    first_row_only: bool = True
    source_query: str | None = None


@dataclass(slots=True, kw_only=True)
class WebActivity(Activity):
    """Web / HTTP activity.

    Attributes:
        url: Target URL.
        method: HTTP method (GET, POST, etc.).
        body: Request body payload.
        headers: HTTP headers.
        authentication: Parsed authentication configuration.
        disable_cert_validation: Skip TLS verification when True.
        http_request_timeout_seconds: Request-level timeout.
    """

    url: str
    method: str
    body: Any = None
    headers: dict[str, str] | None = None
    authentication: dict[str, Any] | None = None
    disable_cert_validation: bool = False
    http_request_timeout_seconds: int | None = None


@dataclass(slots=True, kw_only=True)
class DeleteActivity(Activity):
    """Delete files / folders activity.

    Attributes:
        dataset_name: Reference name of the target dataset.
        folder_path: Folder path to delete within the dataset.
        recursive: Remove contents recursively when True.
    """

    dataset_name: str
    folder_path: str | None = None
    recursive: bool = True


@dataclass(slots=True, kw_only=True)
class ExecutePipelineActivity(Activity):
    """Execute (nested) pipeline activity.

    Attributes:
        pipeline_name: Name of the child pipeline to invoke.
        parameters: Parameters passed to the child pipeline.
        wait_on_completion: Block until the child pipeline finishes.
    """

    pipeline_name: str
    parameters: dict[str, Any] | None = None
    wait_on_completion: bool = True


@dataclass(slots=True, kw_only=True)
class RunJobActivity(Activity):
    """Run an existing Databricks job.

    Attributes:
        job_name: Name of the job to run.
        existing_job_id: ID of an existing job, if known.
        job_parameters: Parameters passed to the job at runtime.
    """

    job_name: str
    existing_job_id: str | None = None
    job_parameters: dict[str, Any] | None = None


@dataclass(slots=True, kw_only=True)
class SparkJarActivity(Activity):
    """Spark JAR activity.

    Attributes:
        main_class_name: Fully qualified main class within the JAR.
        parameters: Arguments passed to the main class.
        libraries: Library descriptors (JARs, wheels, etc.).
    """

    main_class_name: str
    parameters: list[str] | None = None
    libraries: list[dict[str, Any]] | None = None


@dataclass(slots=True, kw_only=True)
class SparkPythonActivity(Activity):
    """Spark Python activity.

    Attributes:
        python_file: Path to the Python file to execute.
        parameters: Arguments passed to the script.
    """

    python_file: str
    parameters: list[str] | None = None


@dataclass(slots=True, kw_only=True)
class SwitchCase:
    """A single case branch within a SwitchActivity.

    Attributes:
        value: The literal value to compare against the switch expression.
        activities: Activities to execute when this case matches.
    """

    value: str
    activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class SwitchActivity(Activity):
    """Switch (multi-branch) activity.

    Evaluates an expression and routes to the matching case branch,
    falling back to default activities if no case matches.

    Attributes:
        on_expression: The ADF expression to evaluate.
        cases: Ordered list of case branches.
        default_activities: Activities to run when no case matches.
    """

    on_expression: str
    cases: list[SwitchCase] = field(default_factory=list)
    default_activities: list[Activity] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class WaitActivity(Activity):
    """Wait / sleep activity.

    Pauses pipeline execution for a specified number of seconds.

    Attributes:
        wait_time_seconds: Duration to wait in seconds.
    """

    wait_time_seconds: int


@dataclass(slots=True, kw_only=True)
class FilterActivity(Activity):
    """Filter activity.

    Applies a condition to an input array, returning only matching items.

    Attributes:
        items_expression: ADF expression for the input array.
        condition_expression: ADF expression for the filter condition.
    """

    items_expression: str
    condition_expression: str


@dataclass(slots=True, kw_only=True)
class AppendVariableActivity(Activity):
    """Append variable activity.

    Adds a value to an existing array variable.

    Attributes:
        variable_name: Name of the array variable.
        append_value: Expression string that evaluates to the value to append.
        value_kind: Kind of the resolved expression ("literal", "dab_ref", "notebook_code").
        notebook_code: Python code for notebook_code kind values.
        notebook_imports: Import statements needed for notebook_code.
    """

    variable_name: str
    append_value: str
    value_kind: str = "literal"  # "literal", "dab_ref", "notebook_code"
    notebook_code: str | None = None
    notebook_imports: list[str] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class UnsupportedActivity(Activity):
    """Sentinel for activities that could not be translated.

    Attributes:
        original_type: The ADF activity type that was not supported.
        reason: Human-readable explanation of why translation failed.
    """

    original_type: str
    reason: str | None = None


@dataclass(slots=True, kw_only=True)
class PlaceholderActivity(Activity):
    """Placeholder notebook for activities that require manual intervention.

    Attributes:
        original_type: The ADF activity type being replaced.
        notebook_path: Workspace path to the placeholder notebook.
        comment: Guidance for the user on what to implement.
    """

    original_type: str
    notebook_path: str = "/UNSUPPORTED_ADF_ACTIVITY"
    comment: str | None = None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class Pipeline:
    """Top-level workflow container produced by the translator.

    Attributes:
        name: Logical pipeline name.
        parameters: Pipeline parameter definitions.
        schedule: Serialized schedule definition, if any.
        tasks: Ordered list of translated activities.
        tags: System and user-defined tags.
        not_translatable: Entries describing properties that could not be translated.
    """

    name: str
    parameters: list[dict[str, Any]] | None = None
    schedule: dict[str, Any] | None = None
    tasks: list[Activity] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    not_translatable: list[dict[str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Translation context (immutable, threaded through visitors)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TranslationContext:
    """Immutable snapshot of translation state threaded through each visitor call.

    Every function that needs to read or extend the caches receives a
    ``TranslationContext`` and returns a new one -- the original is never mutated.

    Attributes:
        activity_cache: Read-only mapping of activity names to translated activities.
        registry: Read-only mapping of activity type strings to translator callables.
        variable_cache: Read-only mapping of variable names to the task keys
            of the tasks that set them.
    """

    activity_cache: MappingProxyType[str, Activity] = field(default_factory=lambda: MappingProxyType({}))
    registry: MappingProxyType[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    variable_cache: MappingProxyType[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def with_activity(self, name: str, activity: Activity) -> TranslationContext:
        """Return a new context with *activity* added to the cache.

        Args:
            name: Activity name used as the cache key.
            activity: Translated activity to store.

        Returns:
            New ``TranslationContext`` containing the updated activity cache.
        """
        return TranslationContext(
            activity_cache=MappingProxyType({**self.activity_cache, name: activity}),
            registry=self.registry,
            variable_cache=self.variable_cache,
        )

    def get_activity(self, activity_name: str) -> Activity | None:
        """Look up a previously translated activity by name.

        Args:
            activity_name: Activity name.

        Returns:
            Cached ``Activity`` or ``None`` if not yet visited.
        """
        return self.activity_cache.get(activity_name)

    def with_variable(self, variable_name: str, task_key: str) -> TranslationContext:
        """Return a new context with a variable mapping added.

        Args:
            variable_name: Variable name.
            task_key: Task key of the task that sets this variable.

        Returns:
            New ``TranslationContext`` containing the updated variable cache.
        """
        return TranslationContext(
            activity_cache=self.activity_cache,
            registry=self.registry,
            variable_cache=MappingProxyType({**self.variable_cache, variable_name: task_key}),
        )

    def get_variable_task_key(self, variable_name: str) -> str | None:
        """Look up the task key that sets a variable.

        Args:
            variable_name: Variable name.

        Returns:
            Cached task key or ``None`` if not found.
        """
        return self.variable_cache.get(variable_name)


# ---------------------------------------------------------------------------
# Translation result types
# ---------------------------------------------------------------------------


TranslationResult: TypeAlias = Activity | UnsupportedActivity


# ---------------------------------------------------------------------------
# Agentic gap tracking
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class AgenticGap:
    """Describes an activity that requires agentic translation.

    Attributes:
        activity_name: Display name of the activity.
        activity_type: ADF activity type string.
        recommended_skill: Skill identifier to use for translation.
        raw_definition: Original ADF JSON definition for the activity.
    """

    activity_name: str
    activity_type: str
    recommended_skill: str | None = None
    raw_definition: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Translation report
# ---------------------------------------------------------------------------


@dataclass(slots=True, kw_only=True)
class TranslationReport:
    """Summary produced after translating an entire ADF pipeline.

    Attributes:
        pipeline: The translated pipeline IR.
        deterministic_count: Activities translated deterministically.
        agentic_count: Activities requiring agentic translation.
        unsupported_count: Activities that could not be translated.
        gaps: List of agentic gaps identified during translation.
        warnings: Human-readable warning messages emitted during translation.
    """

    pipeline: Pipeline
    deterministic_count: int = 0
    agentic_count: int = 0
    unsupported_count: int = 0
    gaps: list[AgenticGap] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
