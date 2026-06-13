"""MCP server exposing orchestra as a single dispatcher tool.

orchestra is driven through one MCP tool, ``orchestra(command, parameters)``, to stay well under
host tool-count limits (e.g. Databricks Genie Code's 20-tools-across-all-servers cap). Each command
is a thin wrapper over ``python -m orchestra.adapter <command> …`` (see :mod:`orchestra.mcp.runner`),
then reads back the JSON/CSV artifacts each phase writes — so the tool stays in lockstep with the
tested CLI and no phase logic is duplicated.

The migration phases share one ``output_dir`` (default ``./orchestra_output``): the generated DAB
bundle at the top level, kept artifacts under ``metadata/``, and transient intermediates under
``.work/``.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from orchestra.mcp import runner


def _allowed_origins() -> list[str]:
    """Allowed browser/MCP origins from ``ORCHESTRA_ALLOWED_ORIGINS`` (comma-separated, default ``*``)."""
    raw = os.environ.get("ORCHESTRA_ALLOWED_ORIGINS", "*")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _transport_security() -> TransportSecuritySettings:
    """Disable the MCP SDK's DNS-rebinding (Host/Origin allowlist) protection.

    A Databricks App is reachable only through the workspace OAuth proxy, which authenticates
    every request before forwarding it to the app on ``localhost:<port>``. That makes the SDK's
    Host/Origin checks both misfire — rejecting the workspace ``Origin`` with 403, or the proxied
    ``Host: localhost:8000`` with 421 — while adding no real protection on top of the proxy. So we
    turn it off. Browser CORS is a separate concern handled in :func:`build_http_app` via
    ``ORCHESTRA_ALLOWED_ORIGINS`` (which does **not** affect this setting).
    """
    return TransportSecuritySettings(enable_dns_rebinding_protection=False)


_INSTRUCTIONS = """\
orchestra translates Azure Data Factory (ADF) pipelines into Databricks Lakeflow Jobs packaged as
Declarative Automation Bundles (DABs). Everything is driven through the single `orchestra` tool:
`orchestra(command="<command>", parameters={...})`.

Typical flow:
  orchestra("inputs", {"phase": "discover"})             # learn a phase's inputs
  orchestra("discover", {"adf_source_path": "...", "output_dir": "..."})
  orchestra("convert", {"output_dir": "..."})
  orchestra("inspect", {"report_path": "<output_dir>/.work/translation_report.json"})
  orchestra("apply_answers", {"report_path": "...", "answers": ["id=value"], "output_dir": "..."})
  orchestra("package", {"output_dir": "...", "catalog": "main", "schema": "default"})
Or run it all at once:
  orchestra("migrate", {"adf_source_path": "...", "output_dir": "...", "catalog": "...", "schema": "..."})

All phases share one output_dir. Provide ADF source paths and output_dir as locations the server can
read/write (a local path, or a Unity Catalog Volume path when the host has volume access).
"""


def _phase_result(result: runner.AdapterResult, output_dir: Path, **extra: Any) -> dict[str, Any]:
    """Assemble a structured tool result from an adapter run plus artifacts."""
    payload: dict[str, Any] = {"ok": result.ok, "process": result.as_dict(), "output_dir": str(output_dir)}
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


def _resolve_source(p: dict[str, Any], path_key: str = "adf_source_path") -> tuple[str | None, Callable[[], None]]:
    """Resolve the ADF source for a command into a local path the adapter can read.

    Input modes, in priority order — a hosted app can't read the user's files directly, so it relies
    on the first three:

    1. ``adf_volume_path`` — a UC Volume directory; the server downloads it via the SDK Files API.
    2. ``adf_workspace_path`` — a ``/Workspace`` directory (e.g. an ADF Git folder); the server
       downloads it via the SDK Workspace API.
       Both (1) and (2) scale to large factories — the bytes bypass the agent. Each returns a temp
       dir + cleanup.
    3. ``adf_definitions`` — an inline ARM-JSON payload (small jobs); materialized to a temp dir.
    4. ``path_key`` (``adf_source_path`` / ``source_dir``) — a path the server itself can read
       (local hosting or a mounted volume).
    """
    if p.get("adf_volume_path"):
        src = runner.download_volume_dir(p["adf_volume_path"])
        return src, lambda: runner.cleanup_materialized(src)
    if p.get("adf_workspace_path"):
        src = runner.download_workspace_dir(p["adf_workspace_path"])
        return src, lambda: runner.cleanup_materialized(src)
    definitions = p.get("adf_definitions")
    if definitions:
        src = runner.materialize_adf_definitions(definitions)
        return src, lambda: runner.cleanup_materialized(src)
    return p.get(path_key), (lambda: None)


def _bundle_output(out: Path, output_volume_path: str | None) -> dict[str, Any]:
    """Deliver the generated bundle: upload to a UC Volume when ``output_volume_path`` is set
    (scales; contents bypass the agent), otherwise return the contents inline."""
    if output_volume_path:
        return {"bundle_uploaded": runner.upload_tree_to_volume(out, output_volume_path)}
    return {"bundle": runner.read_tree(out)}


# --- command handlers (one per adapter operation) -------------------------------------------------
# Each takes the tool's `parameters` dict and returns a structured result. Required keys are accessed
# with `p[...]` so a missing one raises KeyError, which the dispatcher turns into a clear error.


def _cmd_inputs(p: dict[str, Any]) -> dict[str, Any]:
    result = runner.run_adapter(["inputs", p["phase"]])
    return {"ok": result.ok, "inputs": runner.parse_stdout_json(result), "process": result.as_dict()}


def _cmd_discover(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./orchestra_output")
    source, cleanup = _resolve_source(p)
    if not source:
        return {"ok": False, "error": "Provide 'adf_definitions' (inline ARM JSON) or 'adf_source_path'."}
    try:
        args = ["discover", "--adf-source-path", source, "--output-dir", output_dir]
        if p.get("pipeline"):
            args += ["--pipeline", p["pipeline"]]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, inventory=runner.summarize_inventory(out))
    finally:
        cleanup()


def _cmd_convert(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./orchestra_output")
    source, cleanup = _resolve_source(p)
    try:
        args = ["convert", "--output-dir", output_dir]
        if source:
            args += ["--adf-source-path", source]
        if p.get("pipeline"):
            args += ["--pipeline", p["pipeline"]]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, translation=runner.summarize_translation(out))
    finally:
        cleanup()


def _cmd_merge_agentic(p: dict[str, Any]) -> dict[str, Any]:
    args = ["convert", "--merge-agentic", "--report", p["report_path"], "--agentic-results", p["agentic_results_dir"]]
    if p.get("output_path"):
        args += ["--output", p["output_path"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "process": result.as_dict()}


def _cmd_inspect(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["inspect", p["report_path"]]
    for answer in p.get("answers") or []:
        args += ["--answer", answer]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "questions": runner.parse_stdout_json(result), "process": result.as_dict()}


def _cmd_apply_answers(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["modify", p["report_path"]]
    for answer in p["answers"]:
        args += ["--answer", answer]
    if p.get("output_dir"):
        args += ["--output-dir", p["output_dir"]]
    if p.get("lookup_csv"):
        args += ["--lookup-csv", p["lookup_csv"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "process": result.as_dict()}


def _cmd_materialize_lookup(p: dict[str, Any]) -> dict[str, Any]:
    result = runner.run_adapter(["materialize-lookup", p["source"], "--out", p["out"]])
    return {"ok": result.ok, "out": p["out"], "process": result.as_dict()}


def _cmd_workspace_paths(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["workspace-paths", p["report_path"]]
    source, cleanup = _resolve_source(p, path_key="source_dir")
    try:
        if source:
            args += ["--source-dir", source]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "result": runner.parse_stdout_json(result), "process": result.as_dict()}
    finally:
        cleanup()


def _cmd_package(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./orchestra_output")
    args: list[Any] = [
        "package",
        "--output-dir",
        output_dir,
        "--catalog",
        p.get("catalog", "main"),
        "--schema",
        p.get("schema", "default"),
    ]
    if p.get("report_path"):
        args += ["--report", p["report_path"]]
    if p.get("bundle_name"):
        args += ["--bundle-name", p["bundle_name"]]
    if p.get("profile"):
        args += ["--profile", p["profile"]]
    if p.get("download_workspace_files") is False:
        args += ["--no-download-workspace-files"]
    if p.get("keep_intermediates"):
        args += ["--keep-intermediates"]
    result = runner.run_adapter(args)
    out = Path(output_dir)
    setup_md = runner.read_text(out / "SETUP.md") or runner.read_text(out / "setup" / "SETUP.md")
    extra = _bundle_output(out, p.get("output_volume_path")) if result.ok else {}
    return _phase_result(result, out, bundle_files=runner.list_tree(out), setup_md=setup_md, **extra)


def _cmd_migrate(p: dict[str, Any]) -> dict[str, Any]:
    output_dir = p.get("output_dir", "./orchestra_output")
    catalog = p.get("catalog", "main")
    schema = p.get("schema", "default")
    pipeline = p.get("pipeline")
    source, cleanup = _resolve_source(p)
    if not source:
        return {"ok": False, "error": "Provide 'adf_definitions' (inline ARM JSON) or 'adf_source_path'."}
    out = Path(output_dir)
    steps: dict[str, Any] = {}
    try:
        discover_args = ["discover", "--adf-source-path", source, "--output-dir", output_dir]
        if pipeline:
            discover_args += ["--pipeline", pipeline]
        discover_res = runner.run_adapter(discover_args)
        steps["discover"] = _phase_result(discover_res, out, inventory=runner.summarize_inventory(out))
        if not discover_res.ok:
            return {"ok": False, "failed_phase": "discover", "steps": steps}

        convert_args = ["convert", "--output-dir", output_dir, "--adf-source-path", source]
        if pipeline:
            convert_args += ["--pipeline", pipeline]
        convert_res = runner.run_adapter(convert_args)
        steps["convert"] = _phase_result(convert_res, out, translation=runner.summarize_translation(out))
        if not convert_res.ok:
            return {"ok": False, "failed_phase": "convert", "steps": steps}

        package_res = runner.run_adapter(
            ["package", "--output-dir", output_dir, "--catalog", catalog, "--schema", schema]
        )
        extra = _bundle_output(out, p.get("output_volume_path")) if package_res.ok else {}
        steps["package"] = _phase_result(package_res, out, bundle_files=runner.list_tree(out), **extra)
        return {"ok": package_res.ok, "failed_phase": None if package_res.ok else "package", "steps": steps}
    finally:
        cleanup()


def _cmd_record_results(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["record-results", "--output-dir", p["output_dir"], "--results-table", p["results_table"]]
    if p.get("warehouse_id"):
        args += ["--warehouse-id", p["warehouse_id"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "process": result.as_dict()}


def _cmd_install_dashboard(p: dict[str, Any]) -> dict[str, Any]:
    args: list[Any] = ["install-dashboard", "--results-table", p["results_table"]]
    if p.get("warehouse_id"):
        args += ["--warehouse-id", p["warehouse_id"]]
    if p.get("dashboard_name"):
        args += ["--dashboard-name", p["dashboard_name"]]
    if p.get("parent_path"):
        args += ["--parent-path", p["parent_path"]]
    result = runner.run_adapter(args)
    return {"ok": result.ok, "result": runner.parse_stdout_json(result), "process": result.as_dict()}


_COMMANDS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "inputs": _cmd_inputs,
    "discover": _cmd_discover,
    "convert": _cmd_convert,
    "merge_agentic": _cmd_merge_agentic,
    "inspect": _cmd_inspect,
    "apply_answers": _cmd_apply_answers,
    "materialize_lookup": _cmd_materialize_lookup,
    "workspace_paths": _cmd_workspace_paths,
    "package": _cmd_package,
    "migrate": _cmd_migrate,
    "record_results": _cmd_record_results,
    "install_dashboard": _cmd_install_dashboard,
}


def build_server() -> FastMCP:
    """Construct and return the orchestra :class:`FastMCP` server with the single dispatcher tool.

    ``stateless_http=True`` is required by Databricks Genie Code (no persistent ``Mcp-Session-Id``
    round-trip). ``streamable_http_path="/mcp"`` pins the transport to ``/mcp`` (Genie expects the
    server at ``<app-url>/mcp``). ``transport_security`` disables the SDK's DNS-rebinding Origin/Host
    check (see :func:`_transport_security`).
    """
    mcp = FastMCP(
        "orchestra",
        instructions=_INSTRUCTIONS,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=_transport_security(),
    )

    # structured_output=False: do NOT emit an `outputSchema`. FastMCP would otherwise derive one from
    # the `-> dict[str, Any]` return annotation, but Genie Code's MCP client rejects tools that declare
    # an outputSchema (a recent spec feature) — `tools/list` fails and Genie reports "can't fetch tools"
    # even though `initialize` (the connection) succeeded. The dict is still returned, serialized as
    # JSON text content, which every client understands.
    @mcp.tool(structured_output=False)
    def orchestra(command: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run an orchestra ADF→Databricks migration command.

        Call as ``orchestra(command="<command>", parameters={...})``. Commands and their
        ``parameters`` keys (req = required; phases share ``output_dir``, default "./orchestra_output"):

        - "inputs": phase(req: "discover"|"convert"|"package") — list a phase's input prompts.
        - "discover": one of adf_volume_path | adf_workspace_path | adf_definitions | adf_source_path
          (req), output_dir, pipeline — parse ADF JSON, classify activities.
        - "convert": output_dir, (adf_volume_path | adf_workspace_path | adf_definitions |
          adf_source_path), pipeline.
        - "merge_agentic": report_path(req), agentic_results_dir(req), output_path — merge agent results.
        - "inspect": report_path(req), answers(list of "ID=VALUE") — list pending translation options.
        - "apply_answers": report_path(req), answers(req, list of "ID=VALUE"), output_dir, lookup_csv.
        - "materialize_lookup": source(req: CSV path or literal CSV), out(req: destination JSON path).
        - "workspace_paths": report_path(req), (adf_volume_path | adf_workspace_path | adf_definitions
          | source_dir).
        - "package": output_dir, output_volume_path, report_path, catalog(default "main"),
          schema(default "default"), bundle_name, profile, download_workspace_files(bool), keep_intermediates(bool).
        - "migrate": one of adf_volume_path | adf_workspace_path | adf_definitions | adf_source_path
          (req), output_dir, output_volume_path, catalog, schema, pipeline — runs discover→convert→package.
        - "record_results": output_dir(req), results_table(req: catalog.schema.table), warehouse_id.
        - "install_dashboard": results_table(req), warehouse_id, dashboard_name, parent_path.

        Providing the ADF source (a hosted app can't read the user's workspace/volume files directly):
        - ``adf_volume_path``: a UC Volume directory the server reads via the SDK Files API. **Preferred
          for large factories** — the bytes never pass through the agent. Requires the app's service
          principal to have read on the volume.
        - ``adf_workspace_path``: a ``/Workspace`` directory (e.g. an ADF Git folder) the server reads
          via the SDK Workspace API. Also scales (bytes bypass the agent); needs SP read on that path.
        - ``adf_definitions``: an inline mapping of relative path → JSON content mirroring the ADF
          Git-export layout, e.g. {"pipeline/Foo.json": {...}, "linkedService/Bar.json": {...}} (a single
          ARM-template object is also accepted). Convenient for small jobs; capped (~5 MB) since it flows
          through the agent's context — over the cap, switch to ``adf_volume_path``.
        - ``adf_source_path`` / ``source_dir``: a path the server itself can read (local hosting / mounted volume).

        Delivering the generated DAB (the server's output_dir is local/ephemeral):
        - Set ``output_volume_path`` so "package"/"migrate" upload the bundle to a UC Volume and return
          ``bundle_uploaded`` = {"output_volume_path", "files":[...], "count"}. **Preferred for large bundles.**
        - Otherwise they return ``bundle`` = {"files": {relpath: text, ...}, "truncated": [...]} inline.

        Returns a dict ``{"ok": bool, ...}`` with per-command summaries (inventory / translation /
        bundle_files / questions / result) and a "process" block (stdout/stderr/returncode). An unknown
        command, missing required parameter, or an oversized inline payload returns
        ``{"ok": false, "error": ...}``.

        Args:
            command: The operation to run (see the list above).
            parameters: Operation-specific keyword arguments.
        """
        handler = _COMMANDS.get(command)
        if handler is None:
            return {"ok": False, "error": f"Unknown command {command!r}. Valid commands: {', '.join(_COMMANDS)}."}
        try:
            return handler(parameters or {})
        except KeyError as missing:
            return {"ok": False, "error": f"Missing required parameter {missing} for command {command!r}."}
        except ValueError as error:
            return {"ok": False, "error": str(error)}

    return mcp


def build_http_app() -> Any:
    """Build the ASGI app for hosting (Databricks Apps / Genie Code).

    Returns FastMCP's *own* streamable-HTTP app (serving the MCP endpoint at ``/mcp``)
    rather than mounting it inside a separate Starlette app. This is essential: FastMCP's
    StreamableHTTP session manager is started by the app's lifespan, and Starlette does
    **not** run the lifespan of a *mounted* sub-app — so mounting it elsewhere leaves the
    session manager uninitialized and every ``/mcp`` request fails with a 500
    ("Task group is not initialized"), which a client like Genie Code reports as a
    connection failure.

    A ``/`` (and ``/health``) endpoint is registered on the same app via ``custom_route``
    so the platform health check succeeds without a wrapper app. CORS is attached via
    middleware so a browser client (Genie Code) on the workspace origin can reach the
    server; allowed origins default to ``*`` (set ``ORCHESTRA_ALLOWED_ORIGINS`` to a
    comma-separated list, e.g. your workspace URL, to restrict it and enable credentials).
    """
    from starlette.middleware.cors import CORSMiddleware
    from starlette.responses import JSONResponse

    mcp = build_server()

    @mcp.custom_route("/", methods=["GET"])
    async def health(_request: Any) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "mcp-orchestra"})

    @mcp.custom_route("/health", methods=["GET"])
    async def health_alias(_request: Any) -> JSONResponse:
        return JSONResponse({"status": "ok", "service": "mcp-orchestra"})

    # FastMCP's own app — its lifespan starts the StreamableHTTP session manager.
    app = mcp.streamable_http_app()

    allow_origins = _allowed_origins()
    # Credentialed requests cannot use the "*" wildcard per the CORS spec.
    allow_credentials = allow_origins != ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )
    return app


def serve() -> None:
    """Entry point used by ``python -m orchestra.mcp``.

    Defaults to stdio (local agents). With ``--http`` (or ORCHESTRA_MCP_HTTP=1) it serves
    the streamable-HTTP app via uvicorn on ``--port`` / ``$DATABRICKS_APP_PORT`` / 8000.
    """
    import argparse

    parser = argparse.ArgumentParser(prog="python -m orchestra.mcp", description="Run the orchestra MCP server.")
    parser.add_argument("--http", action="store_true", help="Serve over streamable HTTP instead of stdio.")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host for --http mode.")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
        help="Bind port for --http mode (defaults to $DATABRICKS_APP_PORT or 8000).",
    )
    args = parser.parse_args()

    if args.http or os.environ.get("ORCHESTRA_MCP_HTTP") == "1":
        import uvicorn

        uvicorn.run(build_http_app(), host=args.host, port=args.port)
    else:
        build_server().run(transport="stdio")
