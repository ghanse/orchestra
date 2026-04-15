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

# ---------------------------------------------------------------------------
# File-based vs database-based linked-service heuristics
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


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
    by_name: dict[str, AdfActivity] = {a.name: a for a in activities}
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


_Detector = Callable[
    [list[AdfActivity], dict[str, AdfActivity], AdfDefinitions, set[str]],
    list[DetectedMotif],
]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


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
        for inp in activity.inputs:
            ds = definitions.datasets.get(inp.reference_name)
            if ds and ds.linked_service_name:
                ls = definitions.linked_services.get(ds.linked_service_name)
                if ls:
                    if ls.type in _FILE_LS_TYPES:
                        return "files"
                    if ls.type in _DATABASE_LS_TYPES:
                        return "database"
                    if ls.type in _REST_LS_TYPES:
                        return "rest_api"

    # Check linked service directly on the activity
    if activity.linked_service_name:
        ls = definitions.linked_services.get(activity.linked_service_name.reference_name)
        if ls:
            if ls.type in _FILE_LS_TYPES:
                return "files"
            if ls.type in _DATABASE_LS_TYPES:
                return "database"
            if ls.type in _REST_LS_TYPES:
                return "rest_api"

    # Check type_properties source type hints
    tp = activity.type_properties or {}
    source = tp.get("source", {})
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
    return [a for a in activities if a.type == adf_type and a.name not in claimed]


def _has_keyword(text: str, *keywords: str) -> bool:
    """Case-insensitive keyword check in *text*."""
    lower = text.lower()
    return any(kw.lower() in lower for kw in keywords)


# ---------------------------------------------------------------------------
# Individual motif detectors
# ---------------------------------------------------------------------------


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
            act = by_name.get(name)
            if act and act.type == "Lookup" and act.name not in claimed:
                upstream_lookups.append(act)

        if len(upstream_lookups) < 2:
            continue

        # Check for watermark keywords in Lookup queries (but NOT change tracking)
        watermark_keywords_found = False
        has_change_tracking = False
        notes: list[str] = []

        for lk in upstream_lookups:
            tp_text = _type_props_text(lk)
            if _has_keyword(tp_text, "watermark", "max(", "min(", "last_modified", "lastmodified"):
                watermark_keywords_found = True
                notes.append(f"Lookup '{lk.name}' contains watermark-style query")
            if _has_keyword(tp_text, "change_tracking", "changetable", "sys_change_version"):
                has_change_tracking = True

        if has_change_tracking:
            # This is CDC, not watermark -- skip for this detector
            continue

        if not watermark_keywords_found:
            continue

        # Find downstream StoredProcedure
        downstream_sp: AdfActivity | None = None
        for act in activities:
            if act.type == "SqlServerStoredProcedure" and act.name not in claimed:
                if _depends_on(act, copy_act.name):
                    downstream_sp = act
                    break

        if downstream_sp is None:
            continue

        matched = [lk.name for lk in upstream_lookups] + [copy_act.name, downstream_sp.name]
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
            act = by_name.get(name)
            if act and act.type == "Lookup" and act.name not in claimed:
                upstream_lookups.append(act)

        if len(upstream_lookups) < 2:
            continue

        # Must have change-tracking keywords
        cdc_found = False
        notes: list[str] = []
        for lk in upstream_lookups:
            tp_text = _type_props_text(lk)
            if _has_keyword(tp_text, "change_tracking", "changetable", "sys_change_version"):
                cdc_found = True
                notes.append(f"Lookup '{lk.name}' references SQL Server Change Tracking")

        if not cdc_found:
            continue

        # Find downstream StoredProcedure
        downstream_sp: AdfActivity | None = None
        for act in activities:
            if act.type == "SqlServerStoredProcedure" and act.name not in claimed:
                if _depends_on(act, copy_act.name):
                    downstream_sp = act
                    break

        if downstream_sp is None:
            continue

        matched = [lk.name for lk in upstream_lookups] + [copy_act.name, downstream_sp.name]
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
    for_each_acts = _activities_of_type(activities, "ForEach", claimed)

    for fe in for_each_acts:
        # Does ForEach contain a Copy child?
        has_copy_child = False
        if fe.activities:
            for child in fe.activities:
                if child.type == "Copy":
                    has_copy_child = True
                    break
        if not has_copy_child:
            continue

        # Does ForEach have an ExecutePipeline child? If so, this might be
        # parent-child orchestration instead -- skip.
        has_exec_child = False
        if fe.activities:
            for child in fe.activities:
                if child.type == "ExecutePipeline":
                    has_exec_child = True
                    break
        if has_exec_child:
            continue

        # Find upstream Lookup
        upstream_lookups: list[AdfActivity] = []
        for name in _get_upstream_names(fe):
            act = by_name.get(name)
            if act and act.type == "Lookup" and act.name not in claimed:
                upstream_lookups.append(act)

        if not upstream_lookups:
            continue

        notes: list[str] = []
        for lk in upstream_lookups:
            tp_text = _type_props_text(lk)
            if _has_keyword(tp_text, "table", "schema", "control", "metadata", "config"):
                notes.append(f"Lookup '{lk.name}' appears to read a control/metadata table")

        # Even without keyword match we detect if the structure is right
        if not notes:
            notes.append("Lookup -> ForEach -> Copy structure matches bulk copy pattern")

        matched = [lk.name for lk in upstream_lookups] + [fe.name]
        source_hint = None
        # Try to infer from the child Copy
        if fe.activities:
            for child in fe.activities:
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
    get_metadata_acts = _activities_of_type(activities, "GetMetadata", claimed)

    for gm in get_metadata_acts:
        # Check if GetMetadata lists child items (files)
        tp_text = _type_props_text(gm)
        if not _has_keyword(tp_text, "childitems", "getchilditems", "childitem", "exists"):
            # Also accept if it just mentions file-like things
            if not _has_keyword(tp_text, "file", "folder", "blob", "path"):
                continue

        # Walk downstream from GetMetadata
        matched: list[str] = [gm.name]
        notes: list[str] = [f"GetMetadata '{gm.name}' lists files"]

        # Find a direct or indirect downstream ForEach with Copy child
        downstream_filter: AdfActivity | None = None
        downstream_foreach: AdfActivity | None = None
        downstream_delete: AdfActivity | None = None

        for act in activities:
            if act.name in claimed:
                continue
            if act.type == "Filter" and _depends_on(act, gm.name):
                downstream_filter = act
            if act.type == "ForEach":
                # ForEach can depend on GetMetadata directly or via Filter
                deps = _get_upstream_names(act)
                if gm.name in deps or (downstream_filter and downstream_filter.name in deps):
                    # Check for Copy child
                    if act.activities:
                        for child in act.activities:
                            if child.type == "Copy":
                                downstream_foreach = act
                                break

        if downstream_foreach is None:
            continue

        if downstream_filter:
            matched.append(downstream_filter.name)
            notes.append(f"Filter '{downstream_filter.name}' narrows file list")

        matched.append(downstream_foreach.name)
        notes.append(f"ForEach '{downstream_foreach.name}' processes files via Copy")

        # Look for a Delete downstream of the ForEach
        for act in activities:
            if act.name in claimed:
                continue
            if act.type == "Delete" and _depends_on(act, downstream_foreach.name):
                downstream_delete = act
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
        for act in activities:
            if act.name in claimed:
                continue
            if act.type == "WebActivity" and _depends_on(act, copy_act.name):
                downstream_webs.append(act)

        if not downstream_webs:
            continue

        # Check for notification hints
        notes: list[str] = []
        notification_found = False
        for web in downstream_webs:
            tp_text = _type_props_text(web)
            if _has_keyword(tp_text, "logic.azure.com", "email", "notify", "alert", "webhook", "slack", "teams"):
                notification_found = True
                notes.append(f"WebActivity '{web.name}' appears to be a notification call")

        if not notification_found:
            # If there is no notification hint, we still accept if the Web
            # activity depends on Copy with success/failure conditions
            for web in downstream_webs:
                if web.depends_on:
                    for dep in web.depends_on:
                        if dep.activity == copy_act.name and dep.dependency_conditions:
                            conds = [c.lower() for c in dep.dependency_conditions]
                            if "failed" in conds or "completed" in conds:
                                notification_found = True
                                notes.append(
                                    f"WebActivity '{web.name}' triggers on {dep.dependency_conditions} of Copy"
                                )

        if not notification_found:
            continue

        matched = [copy_act.name] + [w.name for w in downstream_webs]
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
            by_name[n] for n in _get_upstream_names(copy_act) if n in by_name and by_name[n].type == "Lookup"
        ]
        if len(upstream_lookups) >= 2:
            continue

        # Find downstream StoredProcedure
        downstream_sp: AdfActivity | None = None
        for act in activities:
            if act.name in claimed:
                continue
            if act.type == "SqlServerStoredProcedure" and _depends_on(act, copy_act.name):
                downstream_sp = act
                break

        if downstream_sp is None:
            continue

        notes: list[str] = []
        tp_text = _type_props_text(copy_act)

        # Check for staging / Synapse hints
        staging_hint = _has_keyword(
            tp_text,
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
            sink_hint = _has_keyword(tp_text, "sqldwsink", "azuresqldwsink", "synapsesink")
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
            act = by_name.get(name)
            if not act or act.name in claimed:
                continue
            if act.type == "WebActivity":
                upstream_webs.append(act)
            elif act.type == "SetVariable":
                upstream_setvars.append(act)

        # Also look for WebActivity upstream of a SetVariable that is upstream of Until
        for sv in list(upstream_setvars):
            for name in _get_upstream_names(sv):
                act = by_name.get(name)
                if act and act.type == "WebActivity" and act.name not in claimed:
                    if act not in upstream_webs:
                        upstream_webs.append(act)

        notes: list[str] = []

        # Require at least some evidence of REST/pagination
        evidence = False
        for web in upstream_webs:
            tp_text = _type_props_text(web)
            if _has_keyword(tp_text, "oauth", "token", "auth", "bearer", "client_id"):
                evidence = True
                notes.append(f"WebActivity '{web.name}' appears to fetch an auth token")

        # Check Until children for pagination keywords
        if until_act.activities:
            for child in until_act.activities:
                tp_text = _type_props_text(child)
                if _has_keyword(tp_text, "page", "cursor", "offset", "skip", "next", "continuation"):
                    evidence = True
                    notes.append(f"Until child '{child.name}' uses pagination")
                if child.type == "SetVariable":
                    sv_text = _type_props_text(child)
                    if _has_keyword(sv_text, "cursor", "page", "offset", "next", "token"):
                        evidence = True
                        notes.append(f"SetVariable '{child.name}' updates pagination cursor")

        if not evidence:
            continue

        matched: list[str] = []
        matched.extend(w.name for w in upstream_webs)
        matched.extend(sv.name for sv in upstream_setvars)
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
    for_each_acts = _activities_of_type(activities, "ForEach", claimed)

    for fe in for_each_acts:
        # Does ForEach contain ExecutePipeline child?
        has_exec_child = False
        if fe.activities:
            for child in fe.activities:
                if child.type == "ExecutePipeline":
                    has_exec_child = True
                    break
        if not has_exec_child:
            continue

        # Find upstream Lookup
        upstream_lookups: list[AdfActivity] = []
        for name in _get_upstream_names(fe):
            act = by_name.get(name)
            if act and act.type == "Lookup" and act.name not in claimed:
                upstream_lookups.append(act)

        if not upstream_lookups:
            continue

        notes: list[str] = [
            f"Lookup '{upstream_lookups[0].name}' provides work items",
            f"ForEach '{fe.name}' iterates and calls child pipelines via ExecutePipeline",
        ]

        matched = [lk.name for lk in upstream_lookups] + [fe.name]

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
    get_metadata_acts = _activities_of_type(activities, "GetMetadata", claimed)

    for gm in get_metadata_acts:
        tp_text = _type_props_text(gm)
        # Must check for existence
        if not _has_keyword(tp_text, "exists"):
            continue

        # Find downstream IfCondition
        downstream_if: AdfActivity | None = None
        for act in activities:
            if act.name in claimed:
                continue
            if act.type == "IfCondition" and _depends_on(act, gm.name):
                downstream_if = act
                break

        if downstream_if is None:
            continue

        notes = [
            f"GetMetadata '{gm.name}' checks file existence",
            f"IfCondition '{downstream_if.name}' gates on the result",
        ]

        matched = [gm.name, downstream_if.name]

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
    dataflow_acts = _activities_of_type(activities, "ExecuteDataFlow", claimed)

    for df_act in dataflow_acts:
        # Find upstream Copy
        upstream_copies: list[AdfActivity] = []
        for name in _get_upstream_names(df_act):
            act = by_name.get(name)
            if act and act.type == "Copy" and act.name not in claimed:
                upstream_copies.append(act)

        if not upstream_copies:
            continue

        notes: list[str] = []
        tp_text = _type_props_text(df_act)
        df_name = df_act.name.lower()

        # Check for SCD hints
        scd_evidence = _has_keyword(
            tp_text + " " + df_name,
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
        notes.append(f"DataFlow '{df_act.name}' performs SCD Type 2 logic")

        matched = [c.name for c in upstream_copies] + [df_act.name]

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
