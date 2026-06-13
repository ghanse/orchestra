"""MCP server exposing orchestra's migration phases and helper operations as tools.

Each tool is a thin wrapper over ``python -m orchestra.adapter`` (see
:mod:`orchestra.mcp.runner`). Tool docstrings are surfaced to the calling agent
as the tool description, so they double as the user-facing contract.

The three migration phases share one ``output_dir`` (default ``./orchestra_output``):
the generated DAB bundle at the top level, kept artifacts under ``metadata/``,
and transient intermediates under ``.work/``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from orchestra.mcp import runner


def _allowed_origins() -> list[str]:
    """Allowed browser/MCP origins from ``ORCHESTRA_ALLOWED_ORIGINS`` (comma-separated, default ``*``)."""
    raw = os.environ.get("ORCHESTRA_ALLOWED_ORIGINS", "*")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _transport_security() -> TransportSecuritySettings:
    """Build the MCP transport security (DNS-rebinding) settings.

    The MCP SDK rejects requests whose ``Origin`` is not in its allowlist with a 403 (and an
    empty allowlist rejects everything). A Databricks App is already fronted by the workspace
    OAuth proxy, so by default we disable the SDK's DNS-rebinding protection — otherwise it
    blocks Genie Code's workspace ``Origin``. Set ``ORCHESTRA_ALLOWED_ORIGINS`` to an explicit
    comma-separated list (e.g. your workspace URL) to instead enforce a strict allowlist.
    """
    origins = _allowed_origins()
    if origins == ["*"]:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    hosts = [netloc for netloc in (urlparse(origin).netloc for origin in origins) if netloc]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_origins=origins,
        allowed_hosts=hosts,
    )


_INSTRUCTIONS = """\
orchestra translates Azure Data Factory (ADF) pipelines into Databricks Lakeflow Jobs
packaged as Declarative Automation Bundles (DABs).

Typical flow:
  1. orchestra_phase_inputs(phase) to learn what each phase needs.
  2. orchestra_profile(...) to parse ADF JSON and classify activities.
  3. orchestra_translate(...) to convert activities to Databricks IR.
  4. orchestra_inspect_report(...) / orchestra_apply_answers(...) to resolve options.
  5. orchestra_prepare(...) to emit the deployable DAB bundle.
Or call orchestra_migrate(...) to run profile -> translate -> prepare with defaults.

All phases operate on a shared output_dir. Provide ADF source paths and output_dir
as locations the server can read/write (a local path, or a Unity Catalog Volume path
when the host has volume access).
"""


def _phase_result(result: runner.AdapterResult, output_dir: Path, **extra: Any) -> dict[str, Any]:
    """Assemble a structured tool result from an adapter run plus artifacts."""
    payload: dict[str, Any] = {"ok": result.ok, "process": result.as_dict(), "output_dir": str(output_dir)}
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


def build_server() -> FastMCP:
    """Construct and return the orchestra :class:`FastMCP` server with all tools registered.

    ``stateless_http=True`` is required by Databricks Genie Code: it only connects to custom
    MCP servers that do not rely on a persistent session (no ``Mcp-Session-Id`` round-trip).
    ``streamable_http_path="/mcp"`` pins the transport to ``/mcp`` (Genie expects the server at
    ``<app-url>/mcp``); without it some SDK versions serve the transport at ``/``.
    ``transport_security`` controls the SDK's DNS-rebinding Origin check (disabled by default so
    the workspace Origin isn't 403'd — see :func:`_transport_security`).
    """
    mcp = FastMCP(
        "orchestra",
        instructions=_INSTRUCTIONS,
        stateless_http=True,
        streamable_http_path="/mcp",
        transport_security=_transport_security(),
    )

    @mcp.tool()
    def orchestra_phase_inputs(phase: str) -> dict[str, Any]:
        """List the input prompts (questions, defaults, descriptions) for a migration phase.

        Args:
            phase: One of "profile", "translate", or "prepare".
        """
        result = runner.run_adapter(["inputs", phase])
        return {"ok": result.ok, "inputs": runner.parse_stdout_json(result), "process": result.as_dict()}

    @mcp.tool()
    def orchestra_profile(
        adf_source_path: str,
        output_dir: str = "./orchestra_output",
        pipeline: str | None = None,
    ) -> dict[str, Any]:
        """Profile (ingest) ADF JSON exports: parse pipelines and classify every activity.

        Writes metadata/inventory.json, metadata/profile_report.csv, and verbatim
        metadata/<pipeline>.arm.json under output_dir.

        Args:
            adf_source_path: Directory (local or UC Volume) holding ADF JSON exports
                (pipeline/, dataset/, linkedService/, trigger/).
            output_dir: Shared migration output directory.
            pipeline: Optional single pipeline name to restrict profiling to.

        Returns:
            Inventory summary (pipeline/activity counts by strategy and coverage).
        """
        args = ["profile", "--adf-source-path", adf_source_path, "--output-dir", output_dir]
        if pipeline:
            args += ["--pipeline", pipeline]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, inventory=runner.summarize_inventory(out))

    @mcp.tool()
    def orchestra_translate(
        output_dir: str = "./orchestra_output",
        adf_source_path: str | None = None,
        pipeline: str | None = None,
    ) -> dict[str, Any]:
        """Translate profiled ADF activities into Databricks intermediate representation (IR).

        Deterministic activity types are translated in-process; agentic gaps are flagged
        in the report for LLM-assisted handling. Writes the transient report to
        output_dir/.work/translation_report.json.

        Args:
            output_dir: Shared migration output directory (must already contain profile output).
            adf_source_path: ADF JSON export directory (defaults to the profiled source).
            pipeline: Optional single pipeline name to restrict translation to.

        Returns:
            Translation summary (pipeline count and per-task status counts).
        """
        args = ["translate", "--output-dir", output_dir]
        if adf_source_path:
            args += ["--adf-source-path", adf_source_path]
        if pipeline:
            args += ["--pipeline", pipeline]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        return _phase_result(result, out, translation=runner.summarize_translation(out))

    @mcp.tool()
    def orchestra_inspect_report(report_path: str, answers: list[str] | None = None) -> dict[str, Any]:
        """Emit the pending translation options (questions) for a translation report as JSON.

        Options whose conditions depend on earlier answers surface only once those answers
        are supplied, so pass the answers gathered so far to reveal chained questions.

        Args:
            report_path: Path to the translation report / pipeline IR JSON.
            answers: Already-collected answers as "OPTION_ID=VALUE" strings.
        """
        args: list[Any] = ["inspect", report_path]
        for answer in answers or []:
            args += ["--answer", answer]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "questions": runner.parse_stdout_json(result), "process": result.as_dict()}

    @mcp.tool()
    def orchestra_apply_answers(
        report_path: str,
        answers: list[str],
        output_dir: str | None = None,
        lookup_csv: str | None = None,
    ) -> dict[str, Any]:
        """Apply collected answers to a translation report and write the configuration-stamped IR.

        Args:
            report_path: Path to the translation report / pipeline IR JSON.
            answers: Collected answers as "OPTION_ID=VALUE" strings (one per answered option).
            output_dir: Migration output directory; the stamped IR is written to its
                .work/translation_report.stamped.json and answers to metadata/configuration.json.
            lookup_csv: Optional CSV file path or literal CSV string for consolidated
                metadata-driven motifs.
        """
        args: list[Any] = ["modify", report_path]
        for answer in answers:
            args += ["--answer", answer]
        if output_dir:
            args += ["--output-dir", output_dir]
        if lookup_csv:
            args += ["--lookup-csv", lookup_csv]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "process": result.as_dict()}

    @mcp.tool()
    def orchestra_materialize_lookup(source: str, out: str) -> dict[str, Any]:
        """Parse CSV-shaped lookup values into the JSON list shape that apply_answers consumes.

        Args:
            source: A CSV file path or a literal CSV string (first row = headers).
            out: Destination path for the lookup-values JSON list.
        """
        result = runner.run_adapter(["materialize-lookup", source, "--out", out])
        return {"ok": result.ok, "out": out, "process": result.as_dict()}

    @mcp.tool()
    def orchestra_workspace_paths(report_path: str, source_dir: str | None = None) -> dict[str, Any]:
        """Detect absolute workspace paths in a report and suggest Databricks hosts from linked services.

        Args:
            report_path: Path to the (stamped) translation report / pipeline IR JSON.
            source_dir: Optional ADF JSON export directory; linked_services/*.json are read
                to suggest the workspace host for `databricks auth login --host`.
        """
        args: list[Any] = ["workspace-paths", report_path]
        if source_dir:
            args += ["--source-dir", source_dir]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "result": runner.parse_stdout_json(result), "process": result.as_dict()}

    @mcp.tool()
    def orchestra_prepare(
        output_dir: str = "./orchestra_output",
        report_path: str | None = None,
        catalog: str = "main",
        schema: str = "default",
        bundle_name: str | None = None,
        profile: str | None = None,
        download_workspace_files: bool = True,
        keep_intermediates: bool = False,
    ) -> dict[str, Any]:
        """Generate the deployable Databricks Asset Bundle (DAB) from the translated IR.

        Emits databricks.yml, resources/ (one job per pipeline), src/ (generated notebooks),
        and setup/ scripts (UC volumes, secrets, connections) under output_dir.

        Args:
            output_dir: Shared migration output directory containing the translation report.
            report_path: Explicit report path (defaults to the stamped/transient report in output_dir).
            catalog: Target Unity Catalog name.
            schema: Target schema name.
            bundle_name: Override the bundle name (defaults to the workflow name).
            profile: Databricks CLI profile used when downloading referenced workspace notebooks.
            download_workspace_files: When False, pass --no-download-workspace-files.
            keep_intermediates: When True, keep the transient .work/ folder.

        Returns:
            The generated bundle file tree plus the SETUP.md / setup-script contents.
        """
        args: list[Any] = ["prepare", "--output-dir", output_dir, "--catalog", catalog, "--schema", schema]
        if report_path:
            args += ["--report", report_path]
        if bundle_name:
            args += ["--bundle-name", bundle_name]
        if profile:
            args += ["--profile", profile]
        if not download_workspace_files:
            args += ["--no-download-workspace-files"]
        if keep_intermediates:
            args += ["--keep-intermediates"]
        result = runner.run_adapter(args)
        out = Path(output_dir)
        setup_md = runner.read_text(out / "SETUP.md") or runner.read_text(out / "setup" / "SETUP.md")
        return _phase_result(result, out, bundle_files=runner.list_tree(out), setup_md=setup_md)

    @mcp.tool()
    def orchestra_migrate(
        adf_source_path: str,
        output_dir: str = "./orchestra_output",
        catalog: str = "main",
        schema: str = "default",
        pipeline: str | None = None,
    ) -> dict[str, Any]:
        """Run the full migration (profile -> translate -> prepare) end to end with defaults.

        This is the non-interactive path: translation options use their defaults (no
        inspect/apply_answers step). For control over options, call the individual phase
        tools and resolve questions with orchestra_inspect_report / orchestra_apply_answers.

        Args:
            adf_source_path: Directory (local or UC Volume) holding ADF JSON exports.
            output_dir: Shared migration output directory.
            catalog: Target Unity Catalog name.
            schema: Target schema name.
            pipeline: Optional single pipeline name to migrate.

        Returns:
            Per-phase summaries and the final bundle file tree.
        """
        out = Path(output_dir)
        steps: dict[str, Any] = {}

        profile_args = ["profile", "--adf-source-path", adf_source_path, "--output-dir", output_dir]
        if pipeline:
            profile_args += ["--pipeline", pipeline]
        profile_res = runner.run_adapter(profile_args)
        steps["profile"] = _phase_result(profile_res, out, inventory=runner.summarize_inventory(out))
        if not profile_res.ok:
            return {"ok": False, "failed_phase": "profile", "steps": steps}

        translate_args = ["translate", "--output-dir", output_dir, "--adf-source-path", adf_source_path]
        if pipeline:
            translate_args += ["--pipeline", pipeline]
        translate_res = runner.run_adapter(translate_args)
        steps["translate"] = _phase_result(translate_res, out, translation=runner.summarize_translation(out))
        if not translate_res.ok:
            return {"ok": False, "failed_phase": "translate", "steps": steps}

        prepare_res = runner.run_adapter(
            ["prepare", "--output-dir", output_dir, "--catalog", catalog, "--schema", schema]
        )
        steps["prepare"] = _phase_result(prepare_res, out, bundle_files=runner.list_tree(out))
        return {"ok": prepare_res.ok, "failed_phase": None if prepare_res.ok else "prepare", "steps": steps}

    @mcp.tool()
    def orchestra_record_results(
        output_dir: str,
        results_table: str,
        warehouse_id: str | None = None,
    ) -> dict[str, Any]:
        """Write per-pipeline migration coverage for this run to a Unity Catalog table.

        Args:
            output_dir: Migration output directory (reads metadata/inventory.json + profile_report.csv).
            results_table: Target UC table as catalog.schema.table.
            warehouse_id: SQL warehouse id (auto-detected, preferring running serverless, when omitted).
        """
        args: list[Any] = ["record-results", "--output-dir", output_dir, "--results-table", results_table]
        if warehouse_id:
            args += ["--warehouse-id", warehouse_id]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "process": result.as_dict()}

    @mcp.tool()
    def orchestra_install_dashboard(
        results_table: str,
        warehouse_id: str | None = None,
        dashboard_name: str | None = None,
        parent_path: str | None = None,
    ) -> dict[str, Any]:
        """Create and publish an AI/BI dashboard visualizing coverage from the results table.

        Args:
            results_table: UC table the dashboard reads (catalog.schema.table).
            warehouse_id: SQL warehouse backing the dashboard (auto-detected when omitted).
            dashboard_name: Dashboard display name.
            parent_path: Workspace folder for the dashboard (defaults to the user's home).
        """
        args: list[Any] = ["install-dashboard", "--results-table", results_table]
        if warehouse_id:
            args += ["--warehouse-id", warehouse_id]
        if dashboard_name:
            args += ["--dashboard-name", dashboard_name]
        if parent_path:
            args += ["--parent-path", parent_path]
        result = runner.run_adapter(args)
        return {"ok": result.ok, "result": runner.parse_stdout_json(result), "process": result.as_dict()}

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
