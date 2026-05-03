"""Heuristic-based motif detection for ADF pipelines.

Walks the ADF activity list and, for each motif definition, checks whether
the activity sequence matches by activity type and dependency chain.  The
matching is deliberately forgiving -- false negatives are acceptable but
false positives are not.

Detection also inspects linked services and datasets to infer a
``source_type_hint`` (``"files"``, ``"database"``, or ``"rest_api"``).
"""

from __future__ import annotations

import logging
from typing import Callable

from orchestra.models.adf_ast import (
    AdfActivity,
    AdfDefinitions,
    AdfPipeline,
)
from orchestra.models.motifs import (
    MOTIF_CDC_CHANGE_TRACKING,
    MOTIF_COPY_AND_NOTIFY,
    MOTIF_FILE_EXISTENCE_VALIDATION,
    MOTIF_FILE_LANDING_ZONE_PROCESSING,
    MOTIF_INCREMENTAL_LOAD_WATERMARK,
    MOTIF_METADATA_DRIVEN_BULK_COPY,
    MOTIF_PARENT_CHILD_ORCHESTRATION,
    MOTIF_REST_API_PAGINATION,
    MOTIF_SCD_TYPE_2,
    MOTIF_STAGED_LOAD_SYNAPSE,
    DetectedMotif,
    MotifDefinition,
)

logger = logging.getLogger(__name__)

_FILE_LS_TYPES: set[str] = {
    "AzureBlobStorage",
    "AzureBlobFS",
    "AzureDataLakeStore",
    "AzureDataLakeStoreGen2",
    "AmazonS3",
    "GoogleCloudStorage",
    "FileServer",
    "FtpServer",
    "Sftp",
    "HttpServer",
}

_DATABASE_LS_TYPES: set[str] = {
    "AzureSqlDatabase",
    "AzureSqlDW",
    "AzureSqlMI",
    "SqlServer",
    "AzureMySql",
    "AzurePostgreSql",
    "Oracle",
    "Db2",
    "Teradata",
    "Snowflake",
    "AmazonRedshift",
    "GoogleBigQuery",
    "AzureCosmosDb",
    "AzureTableStorage",
    "MongoDb",
    "MongoDbAtlas",
    "DynamoDB",
}

_REST_LS_TYPES: set[str] = {
    "RestService",
    "HttpServer",
    "OData",
    "SharePointOnlineList",
}

_Detector = Callable[
    [list[AdfActivity], dict[str, AdfActivity], AdfDefinitions, set[str]],
    list[DetectedMotif],
]


def detect_motifs(
    pipeline: AdfPipeline,
    definitions: AdfDefinitions,
) -> list[DetectedMotif]:
    """Scan *pipeline* for known multi-activity motifs.

    Each motif detector is a specialised heuristic.  Detection order follows
    the ``MOTIF_REGISTRY`` priority so that more specific motifs (e.g.
    CDC Change Tracking) are matched before generic ones (e.g. Copy and
    Notify).  An activity can only belong to one motif -- once claimed it is
    excluded from subsequent matches.

    Args:
        pipeline: Parsed ADF pipeline AST.
        definitions: Full ADF definitions for cross-referencing datasets and
            linked services.

    Returns:
        List of :class:`DetectedMotif` instances, one per matched pattern.
    """
    activities = pipeline.activities
    if not activities:
        return []

    # Build lookup structures
    by_name: dict[str, AdfActivity] = {activity.name: activity for activity in activities}
    claimed: set[str] = set()
    results: list[DetectedMotif] = []

    # Per-motif detectors in priority order
    _detectors: list[tuple[MotifDefinition, _Detector]] = [
        (MOTIF_INCREMENTAL_LOAD_WATERMARK, _detect_incremental_watermark),
        (MOTIF_CDC_CHANGE_TRACKING, _detect_cdc_change_tracking),
        (MOTIF_METADATA_DRIVEN_BULK_COPY, _detect_metadata_driven_bulk_copy),
        (MOTIF_FILE_LANDING_ZONE_PROCESSING, _detect_file_landing_zone),
        (MOTIF_REST_API_PAGINATION, _detect_rest_api_pagination),
        (MOTIF_PARENT_CHILD_ORCHESTRATION, _detect_parent_child_orchestration),
        (MOTIF_FILE_EXISTENCE_VALIDATION, _detect_file_existence_validation),
        (MOTIF_SCD_TYPE_2, _detect_scd_type_2),
        (MOTIF_STAGED_LOAD_SYNAPSE, _detect_staged_load_synapse),
        (MOTIF_COPY_AND_NOTIFY, _detect_copy_and_notify),
    ]

    for motif_def, detector_fn in _detectors:
        matches = detector_fn(activities, by_name, definitions, claimed)
        for match in matches:
            claimed.update(match.matched_activities)
            results.append(match)
            logger.info(
                "Detected motif '%s' in pipeline '%s': activities=%s",
                motif_def.motif_id,
                pipeline.name,
                match.matched_activities,
            )

    return results


def _get_upstream_names(activity: AdfActivity) -> list[str]:
    """Return names of upstream dependencies for *activity*."""
    if not activity.depends_on:
        return []
    return [dep.activity for dep in activity.depends_on]


def _depends_on(
    downstream: AdfActivity,
    upstream_name: str,
) -> bool:
    """Return True if *downstream* directly depends on *upstream_name*."""
    return upstream_name in _get_upstream_names(downstream)


def _type_props_text(activity: AdfActivity) -> str:
    """Flatten type_properties to a lowercase string for keyword searches."""
    if not activity.type_properties:
        return ""
    return str(activity.type_properties).lower()


def _infer_source_type(
    activity: AdfActivity,
    definitions: AdfDefinitions,
) -> str | None:
    """Infer whether the source of a Copy/Lookup activity is files, database, or REST."""
    # Check inputs (dataset references)
    if activity.inputs:
        for input_ref in activity.inputs:
            dataset = definitions.datasets.get(input_ref.reference_name)
            if dataset and dataset.linked_service_name:
                linked_service = definitions.linked_services.get(dataset.linked_service_name)
                if linked_service:
                    if linked_service.type in _FILE_LS_TYPES:
                        return "files"
                    if linked_service.type in _DATABASE_LS_TYPES:
                        return "database"
                    if linked_service.type in _REST_LS_TYPES:
                        return "rest_api"

    # Check linked service directly on the activity
    if activity.linked_service_name:
        linked_service = definitions.linked_services.get(activity.linked_service_name.reference_name)
        if linked_service:
            if linked_service.type in _FILE_LS_TYPES:
                return "files"
            if linked_service.type in _DATABASE_LS_TYPES:
                return "database"
            if linked_service.type in _REST_LS_TYPES:
                return "rest_api"

    # Check type_properties source type hints
    type_properties = activity.type_properties or {}
    source = type_properties.get("source", {})
    if isinstance(source, dict):
        source_type = source.get("type", "")
        if any(
            kw in source_type.lower()
            for kw in ("blob", "s3", "datalake", "file", "parquet", "csv", "json", "avro", "orc")
        ):
            return "files"
        if any(
            kw in source_type.lower()
            for kw in ("sql", "oracle", "db2", "mysql", "postgre", "snowflake", "redshift", "cosmos")
        ):
            return "database"
        if any(kw in source_type.lower() for kw in ("rest", "http", "odata")):
            return "rest_api"

    return None


def _activities_of_type(
    activities: list[AdfActivity],
    adf_type: str,
    claimed: set[str],
) -> list[AdfActivity]:
    """Return unclaimed activities matching *adf_type*."""
    return [activity for activity in activities if activity.type == adf_type and activity.name not in claimed]


def _has_keyword(text: str, *keywords: str) -> bool:
    """Case-insensitive keyword check in *text*."""
    lower = text.lower()
    return any(kw.lower() in lower for kw in keywords)


def _detect_incremental_watermark(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect incremental-load-watermark pattern.

    Heuristic:
    - 2+ Lookup activities upstream of a Copy activity
    - A StoredProcedure downstream of the Copy
    - Lookup type_properties mention "watermark", "MAX(", or similar
    - Exclude if the type_properties mention "CHANGE_TRACKING" (that is CDC)
    """
    results: list[DetectedMotif] = []
    copies = _activities_of_type(activities, "Copy", claimed)

    for copy_act in copies:
        # Find Lookup activities that are upstream of this Copy
        upstream_lookups: list[AdfActivity] = []
        for name in _get_upstream_names(copy_act):
            activity = by_name.get(name)
            if activity and activity.type == "Lookup" and activity.name not in claimed:
                upstream_lookups.append(activity)

        if len(upstream_lookups) < 2:
            continue

        # Check for watermark keywords in Lookup queries (but NOT change tracking)
        watermark_keywords_found = False
        has_change_tracking = False
        notes: list[str] = []

        for lookup_activity in upstream_lookups:
            type_properties_text = _type_props_text(lookup_activity)
            if _has_keyword(type_properties_text, "watermark", "max(", "min(", "last_modified", "lastmodified"):
                watermark_keywords_found = True
                notes.append(f"Lookup '{lookup_activity.name}' contains watermark-style query")
            if _has_keyword(type_properties_text, "change_tracking", "changetable", "sys_change_version"):
                has_change_tracking = True

        if has_change_tracking:
            # This is CDC, not watermark -- skip for this detector
            continue

        if not watermark_keywords_found:
            continue

        # Find downstream StoredProcedure
        downstream_sp: AdfActivity | None = None
        for activity in activities:
            if activity.type == "SqlServerStoredProcedure" and activity.name not in claimed:
                if _depends_on(activity, copy_act.name):
                    downstream_sp = activity
                    break

        if downstream_sp is None:
            continue

        matched = [lookup_activity.name for lookup_activity in upstream_lookups] + [copy_act.name, downstream_sp.name]
        source_hint = _infer_source_type(copy_act, definitions)
        notes.append(f"StoredProcedure '{downstream_sp.name}' updates watermark after Copy")

        results.append(
            DetectedMotif(
                definition=MOTIF_INCREMENTAL_LOAD_WATERMARK,
                matched_activities=matched,
                source_type_hint=source_hint,
                confidence_notes=notes,
            )
        )

    return results


def _detect_cdc_change_tracking(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect CDC change-tracking pattern.

    Very similar to watermark but specifically looks for CHANGETABLE /
    SYS_CHANGE_VERSION keywords.
    """
    results: list[DetectedMotif] = []
    copies = _activities_of_type(activities, "Copy", claimed)

    for copy_act in copies:
        upstream_lookups: list[AdfActivity] = []
        for name in _get_upstream_names(copy_act):
            activity = by_name.get(name)
            if activity and activity.type == "Lookup" and activity.name not in claimed:
                upstream_lookups.append(activity)

        if len(upstream_lookups) < 2:
            continue

        # Must have change-tracking keywords
        cdc_found = False
        notes: list[str] = []
        for lookup_activity in upstream_lookups:
            type_properties_text = _type_props_text(lookup_activity)
            if _has_keyword(type_properties_text, "change_tracking", "changetable", "sys_change_version"):
                cdc_found = True
                notes.append(f"Lookup '{lookup_activity.name}' references SQL Server Change Tracking")

        if not cdc_found:
            continue

        # Find downstream StoredProcedure
        downstream_sp: AdfActivity | None = None
        for activity in activities:
            if activity.type == "SqlServerStoredProcedure" and activity.name not in claimed:
                if _depends_on(activity, copy_act.name):
                    downstream_sp = activity
                    break

        if downstream_sp is None:
            continue

        matched = [lookup_activity.name for lookup_activity in upstream_lookups] + [copy_act.name, downstream_sp.name]
        source_hint = _infer_source_type(copy_act, definitions)
        notes.append(f"StoredProcedure '{downstream_sp.name}' updates change-tracking version")

        results.append(
            DetectedMotif(
                definition=MOTIF_CDC_CHANGE_TRACKING,
                matched_activities=matched,
                source_type_hint=source_hint or "database",
                confidence_notes=notes,
            )
        )

    return results


def _detect_metadata_driven_bulk_copy(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect metadata-driven bulk copy pattern.

    Heuristic:
    - A Lookup activity upstream of a ForEach
    - The ForEach contains a Copy child activity
    - Lookup query mentions table list, control table, or metadata
    """
    results: list[DetectedMotif] = []
    for_each_activities = _activities_of_type(activities, "ForEach", claimed)

    for for_each_activity in for_each_activities:
        # Bulk-copy motif requires the inner body to *be* the Copy: a single
        # Copy child, with no other transform / orchestration activity in the
        # loop body. Patterns like Notebook -> Copy or BuildReport -> Export
        # are not bulk-copy motifs even when an upstream Lookup is present;
        # they are generic "build then archive" pipelines and the user almost
        # never wants the Copy collapsed into a metadata-driven ingestion
        # template that ignores the upstream notebook work.
        inner_activities = list(for_each_activity.activities or [])
        if len(inner_activities) != 1 or inner_activities[0].type != "Copy":
            continue

        # Find upstream Lookup
        upstream_lookups: list[AdfActivity] = []
        for name in _get_upstream_names(for_each_activity):
            activity = by_name.get(name)
            if activity and activity.type == "Lookup" and activity.name not in claimed:
                upstream_lookups.append(activity)

        if not upstream_lookups:
            continue

        notes: list[str] = []
        for lookup_activity in upstream_lookups:
            type_properties_text = _type_props_text(lookup_activity)
            if _has_keyword(type_properties_text, "table", "schema", "control", "metadata", "config"):
                notes.append(f"Lookup '{lookup_activity.name}' appears to read a control/metadata table")

        # Even without keyword match we detect if the structure is right
        if not notes:
            notes.append("Lookup -> ForEach -> Copy structure matches bulk copy pattern")

        matched = [lookup_activity.name for lookup_activity in upstream_lookups] + [for_each_activity.name]
        source_hint = None
        # Try to infer from the child Copy
        if for_each_activity.activities:
            for child in for_each_activity.activities:
                if child.type == "Copy":
                    source_hint = _infer_source_type(child, definitions)
                    break

        results.append(
            DetectedMotif(
                definition=MOTIF_METADATA_DRIVEN_BULK_COPY,
                matched_activities=matched,
                source_type_hint=source_hint,
                confidence_notes=notes,
            )
        )

    return results


def _detect_file_landing_zone(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect file landing zone processing pattern.

    Heuristic:
    - GetMetadata (list files) upstream
    - Optional Filter in between
    - ForEach containing a Copy
    - Optional Delete downstream
    """
    results: list[DetectedMotif] = []
    get_metadata_activities = _activities_of_type(activities, "GetMetadata", claimed)

    for get_metadata_activity in get_metadata_activities:
        # Check if GetMetadata lists child items (files)
        type_properties_text = _type_props_text(get_metadata_activity)
        if not _has_keyword(type_properties_text, "childitems", "getchilditems", "childitem", "exists"):
            # Also accept if it just mentions file-like things
            if not _has_keyword(type_properties_text, "file", "folder", "blob", "path"):
                continue

        # Walk downstream from GetMetadata
        matched: list[str] = [get_metadata_activity.name]
        notes: list[str] = [f"GetMetadata '{get_metadata_activity.name}' lists files"]

        # Find a direct or indirect downstream ForEach with Copy child
        downstream_filter: AdfActivity | None = None
        downstream_foreach: AdfActivity | None = None
        downstream_delete: AdfActivity | None = None

        for activity in activities:
            if activity.name in claimed:
                continue
            if activity.type == "Filter" and _depends_on(activity, get_metadata_activity.name):
                downstream_filter = activity
            if activity.type == "ForEach":
                # ForEach can depend on GetMetadata directly or via Filter
                deps = _get_upstream_names(activity)
                if get_metadata_activity.name in deps or (downstream_filter and downstream_filter.name in deps):
                    # Check for Copy child
                    if activity.activities:
                        for child in activity.activities:
                            if child.type == "Copy":
                                downstream_foreach = activity
                                break

        if downstream_foreach is None:
            continue

        if downstream_filter:
            matched.append(downstream_filter.name)
            notes.append(f"Filter '{downstream_filter.name}' narrows file list")

        matched.append(downstream_foreach.name)
        notes.append(f"ForEach '{downstream_foreach.name}' processes files via Copy")

        # Look for a Delete downstream of the ForEach
        for activity in activities:
            if activity.name in claimed:
                continue
            if activity.type == "Delete" and _depends_on(activity, downstream_foreach.name):
                downstream_delete = activity
                break

        if downstream_delete:
            matched.append(downstream_delete.name)
            notes.append(f"Delete '{downstream_delete.name}' cleans up processed files")

        results.append(
            DetectedMotif(
                definition=MOTIF_FILE_LANDING_ZONE_PROCESSING,
                matched_activities=matched,
                source_type_hint="files",
                confidence_notes=notes,
            )
        )

    return results


def _detect_copy_and_notify(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect copy-and-notify pattern.

    Heuristic:
    - A Copy activity followed by one or more WebActivity calls
    - The WebActivity URL or body hints at notification (Logic App, email, webhook)
    """
    results: list[DetectedMotif] = []
    copies = _activities_of_type(activities, "Copy", claimed)

    for copy_act in copies:
        downstream_webs: list[AdfActivity] = []
        for activity in activities:
            if activity.name in claimed:
                continue
            if activity.type == "WebActivity" and _depends_on(activity, copy_act.name):
                downstream_webs.append(activity)

        if not downstream_webs:
            continue

        # Check for notification hints
        notes: list[str] = []
        notification_found = False
        for web in downstream_webs:
            type_properties_text = _type_props_text(web)
            if _has_keyword(
                type_properties_text, "logic.azure.com", "email", "notify", "alert", "webhook", "slack", "teams"
            ):
                notification_found = True
                notes.append(f"WebActivity '{web.name}' appears to be a notification call")

        if not notification_found:
            # If there is no notification hint, we still accept if the Web
            # activity depends on Copy with success/failure conditions
            for web in downstream_webs:
                if web.depends_on:
                    for dep in web.depends_on:
                        if dep.activity == copy_act.name and dep.dependency_conditions:
                            conds = [cond.lower() for cond in dep.dependency_conditions]
                            if "failed" in conds or "completed" in conds:
                                notification_found = True
                                notes.append(
                                    f"WebActivity '{web.name}' triggers on {dep.dependency_conditions} of Copy"
                                )

        if not notification_found:
            continue

        matched = [copy_act.name] + [web.name for web in downstream_webs]
        source_hint = _infer_source_type(copy_act, definitions)

        results.append(
            DetectedMotif(
                definition=MOTIF_COPY_AND_NOTIFY,
                matched_activities=matched,
                source_type_hint=source_hint,
                confidence_notes=notes,
            )
        )

    return results


def _detect_staged_load_synapse(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect staged-load (Synapse) pattern.

    Heuristic:
    - Copy activity followed by a StoredProcedure
    - Copy sink targets Synapse / SQL DW or uses staging (PolyBase / COPY command)
    - No upstream Lookups with watermark keywords (that would be incremental load)
    """
    results: list[DetectedMotif] = []
    copies = _activities_of_type(activities, "Copy", claimed)

    for copy_act in copies:
        # Skip if there are upstream lookups (likely incremental / CDC)
        upstream_lookups = [
            by_name[name]
            for name in _get_upstream_names(copy_act)
            if name in by_name and by_name[name].type == "Lookup"
        ]
        if len(upstream_lookups) >= 2:
            continue

        # Find downstream StoredProcedure
        downstream_sp: AdfActivity | None = None
        for activity in activities:
            if activity.name in claimed:
                continue
            if activity.type == "SqlServerStoredProcedure" and _depends_on(activity, copy_act.name):
                downstream_sp = activity
                break

        if downstream_sp is None:
            continue

        notes: list[str] = []
        type_properties_text = _type_props_text(copy_act)

        # Check for staging / Synapse hints
        staging_hint = _has_keyword(
            type_properties_text,
            "polybase",
            "staging",
            "enablestaging",
            "sqldw",
            "synapse",
            "copy_command",
            "allowcopycommand",
        )
        if staging_hint:
            notes.append("Copy activity uses staging/PolyBase for Synapse loading")
        else:
            # Also accept if the sink linked service is Synapse / SQL DW
            sink_hint = _has_keyword(type_properties_text, "sqldwsink", "azuresqldwsink", "synapsesink")
            if sink_hint:
                notes.append("Copy sink targets Azure Synapse / SQL DW")
            else:
                notes.append("Copy -> StoredProcedure pattern matches staged load")

        matched = [copy_act.name, downstream_sp.name]
        source_hint = _infer_source_type(copy_act, definitions)

        results.append(
            DetectedMotif(
                definition=MOTIF_STAGED_LOAD_SYNAPSE,
                matched_activities=matched,
                source_type_hint=source_hint,
                confidence_notes=notes,
            )
        )

    return results


def _detect_rest_api_pagination(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect REST API pagination pattern.

    Heuristic:
    - WebActivity (auth / token) early in the chain
    - SetVariable to store the token or cursor
    - Until loop containing Copy or WebActivity for fetching pages
    """
    results: list[DetectedMotif] = []
    until_acts = _activities_of_type(activities, "Until", claimed)

    for until_act in until_acts:
        # Until should contain Copy or WebActivity child
        has_fetch_child = False
        if until_act.activities:
            for child in until_act.activities:
                if child.type in ("Copy", "WebActivity"):
                    has_fetch_child = True
                    break
        if not has_fetch_child:
            continue

        # Look for upstream WebActivity (auth) or SetVariable (cursor init)
        upstream_names = _get_upstream_names(until_act)
        upstream_webs: list[AdfActivity] = []
        upstream_setvars: list[AdfActivity] = []

        for name in upstream_names:
            activity = by_name.get(name)
            if not activity or activity.name in claimed:
                continue
            if activity.type == "WebActivity":
                upstream_webs.append(activity)
            elif activity.type == "SetVariable":
                upstream_setvars.append(activity)

        # Also look for WebActivity upstream of a SetVariable that is upstream of Until
        for set_variable_activity in list(upstream_setvars):
            for name in _get_upstream_names(set_variable_activity):
                activity = by_name.get(name)
                if activity and activity.type == "WebActivity" and activity.name not in claimed:
                    if activity not in upstream_webs:
                        upstream_webs.append(activity)

        notes: list[str] = []

        # Require at least some evidence of REST/pagination
        evidence = False
        for web in upstream_webs:
            type_properties_text = _type_props_text(web)
            if _has_keyword(type_properties_text, "oauth", "token", "auth", "bearer", "client_id"):
                evidence = True
                notes.append(f"WebActivity '{web.name}' appears to fetch an auth token")

        # Check Until children for pagination keywords
        if until_act.activities:
            for child in until_act.activities:
                type_properties_text = _type_props_text(child)
                if _has_keyword(type_properties_text, "page", "cursor", "offset", "skip", "next", "continuation"):
                    evidence = True
                    notes.append(f"Until child '{child.name}' uses pagination")
                if child.type == "SetVariable":
                    set_variable_text = _type_props_text(child)
                    if _has_keyword(set_variable_text, "cursor", "page", "offset", "next", "token"):
                        evidence = True
                        notes.append(f"SetVariable '{child.name}' updates pagination cursor")

        if not evidence:
            continue

        matched: list[str] = []
        matched.extend(web.name for web in upstream_webs)
        matched.extend(set_variable_activity.name for set_variable_activity in upstream_setvars)
        matched.append(until_act.name)

        results.append(
            DetectedMotif(
                definition=MOTIF_REST_API_PAGINATION,
                matched_activities=matched,
                source_type_hint="rest_api",
                confidence_notes=notes,
            )
        )

    return results


def _detect_parent_child_orchestration(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect parent-child orchestration pattern.

    Heuristic:
    - Lookup upstream of ForEach
    - ForEach contains ExecutePipeline child
    """
    results: list[DetectedMotif] = []
    for_each_activities = _activities_of_type(activities, "ForEach", claimed)

    for for_each_activity in for_each_activities:
        # Does ForEach contain ExecutePipeline child?
        has_exec_child = False
        if for_each_activity.activities:
            for child in for_each_activity.activities:
                if child.type == "ExecutePipeline":
                    has_exec_child = True
                    break
        if not has_exec_child:
            continue

        # Find upstream Lookup
        upstream_lookups: list[AdfActivity] = []
        for name in _get_upstream_names(for_each_activity):
            activity = by_name.get(name)
            if activity and activity.type == "Lookup" and activity.name not in claimed:
                upstream_lookups.append(activity)

        if not upstream_lookups:
            continue

        notes: list[str] = [
            f"Lookup '{upstream_lookups[0].name}' provides work items",
            f"ForEach '{for_each_activity.name}' iterates and calls child pipelines via ExecutePipeline",
        ]

        matched = [lookup_activity.name for lookup_activity in upstream_lookups] + [for_each_activity.name]

        results.append(
            DetectedMotif(
                definition=MOTIF_PARENT_CHILD_ORCHESTRATION,
                matched_activities=matched,
                source_type_hint=None,
                confidence_notes=notes,
            )
        )

    return results


def _detect_file_existence_validation(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect file-existence validation pattern.

    Heuristic:
    - GetMetadata checking ``exists`` field
    - IfCondition downstream
    """
    results: list[DetectedMotif] = []
    get_metadata_activities = _activities_of_type(activities, "GetMetadata", claimed)

    for get_metadata_activity in get_metadata_activities:
        type_properties_text = _type_props_text(get_metadata_activity)
        # Must check for existence
        if not _has_keyword(type_properties_text, "exists"):
            continue

        # Find downstream IfCondition
        downstream_if: AdfActivity | None = None
        for activity in activities:
            if activity.name in claimed:
                continue
            if activity.type == "IfCondition" and _depends_on(activity, get_metadata_activity.name):
                downstream_if = activity
                break

        if downstream_if is None:
            continue

        notes = [
            f"GetMetadata '{get_metadata_activity.name}' checks file existence",
            f"IfCondition '{downstream_if.name}' gates on the result",
        ]

        matched = [get_metadata_activity.name, downstream_if.name]

        results.append(
            DetectedMotif(
                definition=MOTIF_FILE_EXISTENCE_VALIDATION,
                matched_activities=matched,
                source_type_hint="files",
                confidence_notes=notes,
            )
        )

    return results


def _detect_scd_type_2(
    activities: list[AdfActivity],
    by_name: dict[str, AdfActivity],
    definitions: AdfDefinitions,
    claimed: set[str],
) -> list[DetectedMotif]:
    """Detect SCD Type 2 pattern.

    Heuristic:
    - Copy activity to staging
    - ExecuteDataFlow downstream
    - DataFlow type_properties mention SCD-like operations (Lookup + AlterRow + Union)
      or the name/description hints at SCD
    """
    results: list[DetectedMotif] = []
    dataflow_activities = _activities_of_type(activities, "ExecuteDataFlow", claimed)

    for dataflow_activity in dataflow_activities:
        # Find upstream Copy
        upstream_copies: list[AdfActivity] = []
        for name in _get_upstream_names(dataflow_activity):
            activity = by_name.get(name)
            if activity and activity.type == "Copy" and activity.name not in claimed:
                upstream_copies.append(activity)

        if not upstream_copies:
            continue

        notes: list[str] = []
        type_properties_text = _type_props_text(dataflow_activity)
        dataflow_name = dataflow_activity.name.lower()

        # Check for SCD hints
        scd_evidence = _has_keyword(
            type_properties_text + " " + dataflow_name,
            "scd",
            "slowly",
            "dimension",
            "alterrow",
            "type2",
            "type_2",
            "surrogate",
            "effective_date",
            "end_date",
            "is_current",
        )

        if not scd_evidence:
            continue

        notes.append(f"Copy '{upstream_copies[0].name}' stages data")
        notes.append(f"DataFlow '{dataflow_activity.name}' performs SCD Type 2 logic")

        matched = [copy.name for copy in upstream_copies] + [dataflow_activity.name]

        results.append(
            DetectedMotif(
                definition=MOTIF_SCD_TYPE_2,
                matched_activities=matched,
                source_type_hint=_infer_source_type(upstream_copies[0], definitions),
                confidence_notes=notes,
            )
        )

    return results


__all__ = [
    "detect_motifs",
]
