"""Collapse detected motifs into MotifActivity IR nodes.

Takes the list of :class:`DetectedMotif` results from the detector and
replaces the matched activity groups in the translated :class:`Pipeline` IR
with single :class:`MotifActivity` nodes.  Unclaimed activities pass through
unchanged.

The collapsed MotifActivity preserves the original activities for reference
and carries metadata about the Databricks replacement strategy so the
bundler and code generator can produce the appropriate output.
"""

from __future__ import annotations

import logging

from orchestra.models.ir import Activity, Dependency, MotifActivity, Pipeline
from orchestra.models.motifs import DetectedMotif

logger = logging.getLogger(__name__)


def collapse_motifs(
    pipeline: Pipeline,
    motifs: list[DetectedMotif],
) -> Pipeline:
    """Replace matched activity groups with MotifActivity nodes.

    For each detected motif, the matched activities are removed from the
    pipeline task list and replaced with a single :class:`MotifActivity`.
    Dependencies are rewired so that:

    - The MotifActivity inherits the *earliest* dependencies of the matched
      group (i.e. dependencies on activities outside the group).
    - Any activity that depended on a matched activity now depends on the
      MotifActivity instead.

    Activities not claimed by any motif pass through unchanged.

    Args:
        pipeline: The translated pipeline IR.
        motifs: Detected motif matches from the detector.

    Returns:
        A new Pipeline with motif activities collapsed.  The original
        pipeline is not mutated.
    """
    if not motifs:
        return pipeline

    claimed_names: set[str] = set()
    for motif in motifs:
        claimed_names.update(motif.matched_activities)

    tasks_by_name: dict[str, Activity] = {task.name: task for task in pipeline.tasks}
    new_tasks: list[Activity] = []
    motif_task_keys: dict[str, str] = {}

    inserted_motifs: set[str] = set()
    for task in pipeline.tasks:
        if task.name in claimed_names:
            detected = _find_motif_for_activity(task.name, motifs)
            if detected is None:
                new_tasks.append(task)
                continue

            motif_id = detected.definition.motif_id
            if motif_id in inserted_motifs:
                continue
            inserted_motifs.add(motif_id)

            motif_activity = _build_motif_activity(detected, tasks_by_name)
            new_tasks.append(motif_activity)

            for matched_name in detected.matched_activities:
                motif_task_keys[matched_name] = motif_activity.task_key
        else:
            new_tasks.append(task)

    _rewire_dependencies(new_tasks, motif_task_keys)

    return Pipeline(
        name=pipeline.name,
        parameters=pipeline.parameters,
        schedule=pipeline.schedule,
        tasks=new_tasks,
        tags=pipeline.tags,
        not_translatable=pipeline.not_translatable,
    )


def _find_motif_for_activity(
    activity_name: str,
    motifs: list[DetectedMotif],
) -> DetectedMotif | None:
    """Find the motif that claimed a given activity."""
    for motif in motifs:
        if activity_name in motif.matched_activities:
            return motif
    return None


def _build_motif_activity(
    motif: DetectedMotif,
    tasks_by_name: dict[str, Activity],
) -> MotifActivity:
    """Build a MotifActivity from a detected motif and the original tasks."""
    definition = motif.definition

    task_key = f"motif_{definition.motif_id}"
    display_name = definition.display_name

    original_activities = [tasks_by_name[name] for name in motif.matched_activities if name in tasks_by_name]

    external_deps = _collect_external_dependencies(
        original_activities,
        set(motif.matched_activities),
    )

    return MotifActivity(
        name=display_name,
        task_key=task_key,
        description=(
            f"Collapsed motif: {definition.display_name}. "
            f"Replaces {len(motif.matched_activities)} ADF activities with "
            f"{definition.databricks_replacement}."
        ),
        depends_on=external_deps,
        motif_id=definition.motif_id,
        display_name=display_name,
        databricks_replacement=definition.databricks_replacement,
        matched_activity_names=list(motif.matched_activities),
        source_type_hint=motif.source_type_hint,
        confidence_notes=list(motif.confidence_notes),
        original_activities=original_activities,
        notebook_template=definition.notebook_template,
        motif_config=_build_motif_config(definition.databricks_replacement, original_activities, task_key),
    )


def _build_motif_config(
    databricks_replacement: str,
    original_activities: list[Activity],
    motif_task_key: str,
) -> dict[str, Any]:
    """Extract motif-specific settings from the activities being collapsed.

    For ``for_each_ingestion``: pulls the driving Lookup's SQL query and
    source type so the generated notebook can fetch the iteration list
    itself — otherwise the ``items`` widget has no upstream writer and the
    motif is a guaranteed no-op.
    """
    # Local import avoids a module-load cycle (collapser → ir → collapser).
    from orchestra.models.ir import CopyActivity, LookupActivity

    if databricks_replacement != "for_each_ingestion":
        return {}

    lookup = next((activity for activity in original_activities if isinstance(activity, LookupActivity)), None)
    copy = next((activity for activity in original_activities if isinstance(activity, CopyActivity)), None)

    config: dict[str, Any] = {}
    if lookup is not None:
        if lookup.source_query:
            config["lookup_query"] = lookup.source_query
        if lookup.source_type:
            config["lookup_source_type"] = lookup.source_type
        config["lookup_scope"] = lookup.task_key or motif_task_key
    if copy is not None:
        sink_properties = copy.sink_properties or {}
        sink_table = sink_properties.get("table") or sink_properties.get("tableName")
        if sink_table:
            config["sink_table"] = sink_table
        if copy.source_type:
            config["copy_source_type"] = copy.source_type
        config["copy_scope"] = copy.task_key or motif_task_key
    return config


def _collect_external_dependencies(
    activities: list[Activity],
    matched_names: set[str],
) -> list[Dependency]:
    """Collect dependencies that point outside the matched activity group.

    These become the MotifActivity's upstream dependencies — preserving
    the pipeline's overall execution order.
    """
    seen: set[str] = set()
    external_deps: list[Dependency] = []

    for activity in activities:
        if not activity.depends_on:
            continue
        for dep in activity.depends_on:
            if dep.task_key not in matched_names and dep.task_key not in seen:
                seen.add(dep.task_key)
                external_deps.append(Dependency(task_key=dep.task_key, outcome=dep.outcome))

    return external_deps


def _rewire_dependencies(
    tasks: list[Activity],
    motif_task_keys: dict[str, str],
) -> None:
    """Rewire dependencies so activities that depended on collapsed activities
    now depend on the corresponding MotifActivity.

    Mutates ``depends_on`` lists in place.
    """
    for task in tasks:
        if not task.depends_on:
            continue
        new_deps: list[Dependency] = []
        seen_keys: set[str] = set()
        for dep in task.depends_on:
            replacement_key = motif_task_keys.get(dep.task_key)
            effective_key = replacement_key if replacement_key else dep.task_key
            if effective_key not in seen_keys:
                seen_keys.add(effective_key)
                new_deps.append(Dependency(task_key=effective_key, outcome=dep.outcome))
        task.depends_on = new_deps
