"""Generates Python notebook content for activities that need custom notebooks.

Each generator produces a self-contained Databricks notebook with:
- A ``# Databricks notebook source`` header
- Imports for required modules
- ``dbutils.widgets.get()`` for runtime parameters
- ``dbutils.secrets.get()`` for credentials
- ``dbutils.jobs.taskValues.set(...)`` to forward results to downstream tasks
"""

from __future__ import annotations

import json
import re
import textwrap
from typing import TYPE_CHECKING, Any

from orchestra.models.ir import TranslationContext
from orchestra.models.source_types import FILE_SOURCE_TYPES, JDBC_SOURCE_TYPES, REST_SOURCE_TYPES
from orchestra.parser.expression_parser import (
    resolve_expression,
    resolve_interpolated_string_for_notebook,
)

if TYPE_CHECKING:
    from orchestra.models.ir import (
        AppendVariableActivity,
        CopyActivity,
        DeleteActivity,
        FilterActivity,
        LookupActivity,
        SetVariableActivity,
        WaitActivity,
        WebActivity,
    )


def generate_lookup_notebook(activity: LookupActivity, *, scope: str = "") -> str:
    """Generates a Python notebook that executes a lookup query.

    Args:
        activity: The LookupActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Lookup: {activity.name}")
    source_type = activity.source_type or ""
    query = activity.source_query or ""

    is_dynamic_query = "dbutils.widgets.get" in query or "dbutils.jobs.taskValues" in query

    if source_type in JDBC_SOURCE_TYPES:
        scope = scope or activity.task_key

        if is_dynamic_query:
            query_assignment = f"query = {query}"
        else:
            query_assignment = f'query = """{query}"""'

        body = textwrap.dedent(f"""\
            import json

            # Parameters
            first_row_only = dbutils.widgets.get("first_row_only") == "true"

            # Credentials
            jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc-url")
            jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc-password")
            jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc-user")

            # Execute lookup query
            {query_assignment}

            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .option("query", query)
                .load()
            )

            if first_row_only:
                result = df.first()
                output = result.asDict() if result else {{}}
            else:
                output = [row.asDict() for row in df.collect()]

            # Set task values so downstream tasks can reference them via
            # {{{{tasks.{activity.task_key}.values.<key>}}}}.
            # The full result is stored under "result" as a JSON string so
            # for_each_task can consume it as `inputs`.  For firstRow lookups,
            # each column is also stored as an individual task value so
            # condition_task can reference e.g. {{{{tasks.{activity.task_key}.values.cnt}}}}.
            dbutils.jobs.taskValues.set(key="result", value=json.dumps(output))
            if first_row_only and isinstance(output, dict):
                for col_name, col_value in output.items():
                    dbutils.jobs.taskValues.set(key=col_name, value=col_value)
        """)
    else:
        body = textwrap.dedent(f"""\
            import json

            # Parameters
            first_row_only = dbutils.widgets.get("first_row_only") == "true"

            # Execute lookup query via Spark SQL
            query = \"\"\"{query}\"\"\"
            df = spark.sql(query)

            if first_row_only:
                result = df.first()
                output = result.asDict() if result else {{}}
            else:
                output = [row.asDict() for row in df.collect()]

            # Set task values so downstream tasks can reference them via
            # {{{{tasks.{activity.task_key}.values.<key>}}}}.
            dbutils.jobs.taskValues.set(key="result", value=json.dumps(output))
            if first_row_only and isinstance(output, dict):
                for col_name, col_value in output.items():
                    dbutils.jobs.taskValues.set(key=col_name, value=col_value)
        """)

    return header + _command_separator() + body


def generate_web_activity_notebook(activity: WebActivity, *, scope: str = "") -> str:
    """Generates a Python notebook that makes an HTTP request.

    Args:
        activity: The WebActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Web Activity: {activity.name}")

    # Resolve header values — some may contain ADF expressions like
    # {"Authorization": {"type": "Expression", "value": "@concat('Bearer ', ...)"}}
    headers_literal, headers_preamble = _resolve_headers(activity.headers)

    auth_block = ""
    auth = activity.authentication
    if auth:
        scope = scope or activity.task_key
        auth_type = auth.get("type", "")
        if auth_type in ("ServicePrincipal", "MSI", "ManagedServiceIdentity"):
            auth_block = textwrap.dedent(f"""\
                # Authentication ({auth_type})
                auth_token = dbutils.secrets.get(scope="{scope}", key="auth-credential")
                headers["Authorization"] = f"Bearer {{auth_token}}"
            """)
        elif auth_type == "Basic":
            auth_block = textwrap.dedent(f"""\
                # Authentication (Basic)
                import base64
                username = dbutils.secrets.get(scope="{scope}", key="auth-username")
                password = dbutils.secrets.get(scope="{scope}", key="auth-credential")
                token = base64.b64encode(f"{{username}}:{{password}}".encode()).decode()
                headers["Authorization"] = f"Basic {{token}}"
            """)
        else:
            auth_block = textwrap.dedent(f"""\
                # Authentication
                auth_credential = dbutils.secrets.get(scope="{scope}", key="auth-credential")
                headers["Authorization"] = f"Bearer {{auth_credential}}"
            """)

    body_block = ""
    request_call = ""
    if activity.method in ("POST", "PUT", "PATCH"):
        raw_body = activity.body
        # If the body was pre-resolved to Python code by the translator
        # (contains function calls like __import__ or json.loads), embed directly.
        if isinstance(raw_body, str) and ("__import__" in raw_body or "json.loads" in raw_body):
            body_block = f"body = {raw_body}\n"
        else:
            body_str = _resolve_body(raw_body)
            # ``_resolve_body`` may return either a JSON literal, a Python
            # dict literal, or a ``repr()``'d string containing Python-like
            # concat syntax.  Parse strings as JSON when possible so the
            # downstream ``requests.request(json=...)`` gets a real object.
            body_block = textwrap.dedent(f"""\
                body_raw = dbutils.widgets.get("body") or {body_str}
                if isinstance(body_raw, str):
                    try:
                        body = json.loads(body_raw)
                    except (ValueError, TypeError):
                        body = body_raw
                else:
                    body = body_raw
            """)
        # ``json=`` encodes dicts; string bodies go over ``data=`` so we don't
        # double-encode them as JSON strings.
        request_call = textwrap.dedent(
            """\
            if isinstance(body, (dict, list)):
                response = requests.request(method, url, headers=headers, json=body, timeout=300)
            else:
                response = requests.request(method, url, headers=headers, data=body, timeout=300)
            """
        )
    else:
        request_call = "response = requests.request(method, url, headers=headers, timeout=300)\n"

    # When the URL is a DAB dynamic ref ({{...}}), it will be resolved and
    # passed via base_parameters at runtime — just read from the widget.
    if "{{" in activity.url:
        url_line = 'url = dbutils.widgets.get("url")'
    else:
        url_line = f'url = dbutils.widgets.get("url") or "{activity.url}"'

    if "{{" in activity.method:
        method_line = 'method = dbutils.widgets.get("method")'
    else:
        method_line = f'method = dbutils.widgets.get("method") or "{activity.method}"'

    body = textwrap.dedent(f"""\
        import json
        import requests

        # Parameters
        {url_line}
        {method_line}
        headers = {headers_literal}

    """)
    body += headers_preamble
    body += auth_block
    body += body_block
    body += request_call
    body += textwrap.dedent("""\

        response.raise_for_status()

        # Return response
        try:
            result = response.json()
        except ValueError:
            result = {"status_code": response.status_code, "text": response.text}

    """)

    return header + _command_separator() + body


def generate_delete_notebook(activity: DeleteActivity) -> str:
    """Generates a notebook using dbutils.fs.rm().

    Args:
        activity: The DeleteActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Delete: {activity.name}")
    folder_path = activity.folder_path or ""

    body = textwrap.dedent(f"""\
        # Parameters
        dataset_name = dbutils.widgets.get("dataset_name") or "{activity.dataset_name}"
        folder_path = dbutils.widgets.get("folder_path") or "{folder_path}"
        recursive = dbutils.widgets.get("recursive") == "true"

        # Build the full path to delete
        target_path = folder_path if folder_path else dataset_name
        print(f"Deleting: {{target_path}} (recursive={{recursive}})")
        result = dbutils.fs.rm(target_path, recurse=recursive)

    """)

    return header + _command_separator() + body


def generate_set_variable_notebook(activity: SetVariableActivity) -> str:
    """Generates a notebook that sets a task value.

    Databricks jobs use ``dbutils.jobs.taskValues.set()`` as the equivalent
    of ADF pipeline variables, allowing downstream tasks to read values via
    ``dbutils.jobs.taskValues.get()`` or ``{{tasks.<key>.values.<name>}}``.

    When ``value_kind`` is ``"literal"`` or ``"dab_ref"`` the value is read
    from the ``value`` widget parameter (passed via ``base_parameters``).

    When ``value_kind`` is ``"notebook_code"`` the Python code is embedded
    directly in the notebook body -- no executable code in parameters.

    Args:
        activity: The SetVariableActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Set Variable: {activity.name}")

    if activity.value_kind == "notebook_code" and activity.notebook_code:
        # Embed imports and Python code directly in the notebook
        import_lines = "\n".join(activity.notebook_imports) if activity.notebook_imports else ""
        if import_lines:
            import_block = import_lines + "\n"
        else:
            import_block = ""

        # Build body lines list to avoid textwrap.dedent issues when
        # import_block starts at column 0 (which would prevent dedent
        # from stripping the common leading whitespace).
        lines = ["import json"]
        if import_block:
            lines.append(import_block.rstrip("\n"))
        lines.append("")
        lines.append(f'variable_name = "{activity.variable_name}"')
        lines.append("")
        lines.append("# Compute value at runtime.")
        lines.append("# Original ADF expression resolved to notebook code.")
        lines.append(f"value = {activity.notebook_code}")
        lines.append("")
        lines.append("# Set task value so downstream tasks can reference it via")
        lines.append(f"# {{{{tasks.{activity.task_key}.values.{activity.variable_name}}}}}.")
        lines.append("dbutils.jobs.taskValues.set(key=variable_name, value=value)")
        lines.append("print(f\"Set task value '{variable_name}' = '{value}'\")")
        lines.append("")
        body = "\n".join(lines) + "\n"
    else:
        # literal or dab_ref: read from widget parameter
        body = textwrap.dedent(f"""\
            import json

            variable_name = "{activity.variable_name}"

            # Read value from widget parameter (set via base_parameters).
            # DAB resolves dynamic references (e.g. {{{{job.run_id}}}}) before passing.
            value = dbutils.widgets.get("value")

            # Set task value so downstream tasks can reference it via
            # {{{{tasks.{activity.task_key}.values.{activity.variable_name}}}}}.
            dbutils.jobs.taskValues.set(key=variable_name, value=value)
            print(f"Set task value '{{variable_name}}' = '{{value}}'")

        """)

    return header + _command_separator() + body


def generate_wait_notebook(activity: WaitActivity) -> str:
    """Generates a notebook that sleeps for a specified duration.

    ADF Wait activities pause pipeline execution for N seconds.  The
    Databricks equivalent is a simple ``time.sleep()`` call inside a
    generated notebook.

    Args:
        activity: The WaitActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Wait: {activity.name}")

    body = textwrap.dedent(f"""\
        import time

        # Parameters
        default_wait = {activity.wait_time_seconds}
        param = dbutils.widgets.get("wait_seconds")
        wait_seconds = int(param) if param else default_wait

        print(f"Waiting for {{wait_seconds}} seconds...")
        time.sleep(wait_seconds)
        print("Wait complete.")

    """)

    return header + _command_separator() + body


def generate_copy_notebook(activity: CopyActivity, *, scope: str = "") -> str:
    """Generates a notebook for copy operations (Auto Loader, COPY INTO, or JDBC).

    The ingestion strategy is chosen based on the source type string:
    - **File-based sources** (BlobSource, AzureBlobFSSource, etc.): Auto Loader
      for streaming ingestion into a Delta table.
    - **Database sources** (AzureSqlSource, SqlServerSource, etc.): JDBC read
      into a Delta table.
    - **REST sources**: HTTP-based ingestion.
    - **Other**: Fallback Spark read/write with configurable format.

    Args:
        activity: The CopyActivity IR node.
        scope: Secret scope name (defaults to task_key if empty).

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Copy: {activity.name}")
    source_type = activity.source_type or ""

    if source_type in FILE_SOURCE_TYPES:
        body = _generate_autoloader_body(activity)
    elif source_type in JDBC_SOURCE_TYPES:
        body = _generate_jdbc_body(activity, scope=scope)
    elif source_type in REST_SOURCE_TYPES:
        body = _generate_rest_copy_body(activity)
    else:
        body = _generate_generic_copy_body(activity)

    # Hoist any imports the body needs into a single cell at the top of
    # the notebook.  ``_render_sink_write`` and a few other helpers used
    # to inline ``from datetime import ...`` next to the call site, which
    # produced an awkward block sandwiched between two comment groups.
    imports = _detect_imports(body)
    if imports:
        body = _strip_inline_imports(body, imports)
        return header + _command_separator() + "\n".join(imports) + "\n" + _command_separator() + body
    return header + _command_separator() + body


def _detect_imports(body: str) -> list[str]:
    """Return import lines required by code references found in *body*."""
    needed: list[str] = []
    if "datetime." in body:
        needed.append("from datetime import datetime, timezone, timedelta")
    if "ZoneInfo(" in body:
        needed.append("from zoneinfo import ZoneInfo")
    return needed


def _strip_inline_imports(body: str, hoisted: list[str]) -> str:
    """Remove inline import lines that match the ones we hoisted to the top."""
    lines = body.splitlines()
    keep: list[str] = []
    skip_prefixes = (
        "from datetime import",
        "from zoneinfo import",
        "import datetime",
    )
    for line in lines:
        stripped = line.lstrip()
        if any(stripped.startswith(prefix) for prefix in skip_prefixes):
            continue
        keep.append(line)
    return "\n".join(keep) + ("\n" if body.endswith("\n") else "")


def generate_filter_notebook(activity: FilterActivity) -> str:
    """Generates a notebook that filters an array and stores the result as a task value.

    The notebook reads the input array from a task value or parameter, applies
    the filter condition, and writes the filtered result via
    ``dbutils.jobs.taskValues.set()``.

    Args:
        activity: The FilterActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Filter: {activity.name}")

    body = textwrap.dedent(f"""\
        import json

        # Parameters
        items_expression = dbutils.widgets.get("items_expression") or '''{activity.items_expression}'''
        condition_expression = dbutils.widgets.get("condition_expression") or '''{activity.condition_expression}'''

        # Evaluate the input array
        # If the items expression is a task value reference, evaluate it;
        # otherwise treat it as a JSON-encoded list.
        try:
            items = eval(items_expression)
        except Exception:
            items = json.loads(items_expression)

        if not isinstance(items, list):
            items = list(items) if hasattr(items, '__iter__') else [items]

        # Apply the filter condition
        # The condition expression should be a Python lambda or expression string.
        # For ADF @equals(item().status, 'active') style conditions, we wrap in a
        # lambda that receives each item.
        try:
            filter_fn = eval(f"lambda item: {{condition_expression}}")
            filtered = [item for item in items if filter_fn(item)]
        except Exception:
            # Fallback: keep all items if the condition cannot be evaluated
            print(f"WARNING: Could not evaluate filter condition: {{condition_expression}}")
            print("Returning all items unfiltered. Please review the condition expression.")
            filtered = items

        # Set the filtered result as a task value for downstream tasks
        result = json.dumps(filtered)
        dbutils.jobs.taskValues.set(key="output", value=result)
        print(f"Filtered {{len(items)}} items to {{len(filtered)}} items")

    """)

    return header + _command_separator() + body


def generate_append_variable_notebook(activity: AppendVariableActivity) -> str:
    """Generates a notebook that appends a value to an array task value.

    Reads the current array from a task value (or initialises an empty list),
    appends the new value, and writes the updated array back via
    ``dbutils.jobs.taskValues.set()``.

    When ``value_kind`` is ``"notebook_code"`` the Python code is embedded
    directly -- no executable code in parameters.

    Args:
        activity: The AppendVariableActivity IR node.

    Returns:
        Complete notebook source code as a string.
    """
    header = _notebook_header(f"Append Variable: {activity.name}")

    if activity.value_kind == "notebook_code" and activity.notebook_code:
        import_lines = "\n".join(activity.notebook_imports) if activity.notebook_imports else ""
        if import_lines:
            import_block = import_lines + "\n"
        else:
            import_block = ""

        # Build body lines list to avoid textwrap.dedent issues when
        # import_block starts at column 0.
        lines = ["import json"]
        if import_block:
            lines.append(import_block.rstrip("\n"))
        lines.append("")
        lines.append("# Parameters")
        lines.append(f'variable_name = dbutils.widgets.get("variable_name") or "{activity.variable_name}"')
        lines.append("")
        lines.append("# Compute value at runtime.")
        lines.append(f"value = {activity.notebook_code}")
        lines.append("")
        lines.append("# Read the current array from task values (or start with empty list)")
        lines.append("# `source_task_key` is populated at deploy time with the task that most")
        lines.append("# recently set this variable.  An empty value falls back to [].")
        lines.append("source_task_key = dbutils.widgets.get('source_task_key')")
        lines.append("current: list = []")
        lines.append("if source_task_key:")
        lines.append("    try:")
        lines.append("        current_raw = dbutils.jobs.taskValues.get(taskKey=source_task_key, key=variable_name)")
        lines.append("        if isinstance(current_raw, str):")
        lines.append("            current = json.loads(current_raw)")
        lines.append("        elif isinstance(current_raw, list):")
        lines.append("            current = current_raw")
        lines.append("        elif current_raw is not None:")
        lines.append("            current = [current_raw]")
        lines.append("    except Exception:")
        lines.append("        current = []")
        lines.append("")
        lines.append("if not isinstance(current, list):")
        lines.append("    current = [current] if current else []")
        lines.append("")
        lines.append("# Append and write back")
        lines.append("current.append(value)")
        lines.append("result = json.dumps(current)")
        lines.append("dbutils.jobs.taskValues.set(key=variable_name, value=result)")
        lines.append("print(f\"Appended to '{variable_name}': array now has {len(current)} item(s)\")")
        lines.append("")
        body = "\n".join(lines) + "\n"
    else:
        body = textwrap.dedent(f"""\
            import json

            # Parameters
            variable_name = dbutils.widgets.get("variable_name") or "{activity.variable_name}"
            value = dbutils.widgets.get("value") or {json.dumps(activity.append_value)}

            # Read the current array from a prior task's values.
            # `source_task_key` is passed via base_parameters and points at the
            # task that most recently set this variable; empty → start with [].
            source_task_key = dbutils.widgets.get("source_task_key")
            current: list = []
            if source_task_key:
                try:
                    current_raw = dbutils.jobs.taskValues.get(taskKey=source_task_key, key=variable_name)
                    if isinstance(current_raw, str):
                        current = json.loads(current_raw)
                    elif isinstance(current_raw, list):
                        current = current_raw
                    elif current_raw is not None:
                        current = [current_raw]
                except Exception:
                    current = []

            # Evaluate the value to append
            try:
                starts = ('{{', '[', '"')
                append_val = json.loads(value) if isinstance(value, str) and value.startswith(starts) else value
            except (json.JSONDecodeError, ValueError):
                append_val = value

            # Append and write back
            current.append(append_val)
            result = json.dumps(current)
            dbutils.jobs.taskValues.set(key=variable_name, value=result)
            print(f"Appended to '{{variable_name}}': array now has {{len(current)}} item(s)")

        """)

    return header + _command_separator() + body


def _notebook_header(title: str) -> str:
    """Return the standard Databricks notebook header block."""
    return textwrap.dedent(f"""\
        # Databricks notebook source
        # MAGIC %md
        # MAGIC # {title}
        # MAGIC
        # MAGIC *Auto-generated by Orchestra. Do not edit manually unless necessary.*
    """)


def _command_separator() -> str:
    """Return the Databricks cell separator comment."""
    return "\n# COMMAND ----------\n\n"


_DAB_REF_PARAMETER_RE = re.compile(r"\{\{job\.parameters\.(\w+)\}\}")
_DAB_REF_RUN_ID_RE = re.compile(r"\{\{job\.run_id\}\}")
_DAB_REF_JOB_NAME_RE = re.compile(r"\{\{job\.name\}\}")
_DAB_REF_START_TIME_RE = re.compile(r"\{\{job\.start_time\.iso_datetime\}\}")
_DAB_REF_TASK_VALUE_RE = re.compile(r"\{\{tasks\.([^.]+)\.values\.(\w+)\}\}")


def _dab_ref_to_fstring_expr(ref: str) -> str:
    """Converts a DAB ref (``{{job.name}}``) to a Python f-string expression.

    The returned snippet is meant to be inlined in an f-string, so it always
    evaluates to a string at notebook runtime via ``dbutils`` or
    ``spark.conf.get``.
    """
    parameter_match = _DAB_REF_PARAMETER_RE.match(ref)
    if parameter_match:
        return "{dbutils.widgets.get('" + parameter_match.group(1) + "')}"
    if _DAB_REF_RUN_ID_RE.match(ref):
        return "{spark.conf.get('spark.databricks.job.runId', 'unknown')}"
    if _DAB_REF_JOB_NAME_RE.match(ref):
        return "{spark.conf.get('spark.databricks.job.parentName', 'unknown')}"
    if _DAB_REF_START_TIME_RE.match(ref):
        return "{spark.conf.get('spark.databricks.job.triggerTime', 'unknown')}"
    task_value_match = _DAB_REF_TASK_VALUE_RE.match(ref)
    if task_value_match:
        task_key, value_key = task_value_match.group(1), task_value_match.group(2)
        return "{dbutils.jobs.taskValues.get(taskKey='" + task_key + "', key='" + value_key + "')}"
    return ref


def _resolve_body(body: Any) -> str:
    """Resolves ADF expressions in a request body and return a Python expression.

    The returned string must be valid Python that evaluates (at notebook
    execution time) to a ``dict`` / ``list`` / ``str`` — whatever
    ``requests.request(json=..., data=...)`` can send.  Callers are
    responsible for typing the result (``isinstance(body, (dict, list))``).

    Two resolver outcomes to be careful about:

    1. ``resolve_expression`` returns ``kind="notebook_code"`` — the value is
       already Python code that produces the right runtime value; embed
       directly.
    2. ``resolve_expression`` returns ``kind="literal"`` — the value is a
       plain Python scalar.  Use ``json.dumps`` so string / number / bool
       are all turned into valid Python source literals.  Using ``repr``
       here is a trap: ``repr`` of a string containing Python source
       produces a *string literal* of that source, not the source itself.

    Args:
        body: The raw body from the WebActivity IR.

    Returns:
        A Python expression string suitable for embedding in generated code.
    """
    if body is None:
        return "None"
    if isinstance(body, str):
        return _resolve_string_body(body)
    if isinstance(body, dict):
        return _resolve_dict_body(body)
    return json.dumps(body) if body else "''"


def _resolve_string_body(body: str) -> str:
    """Renders a string-shaped WebActivity body as Python source."""
    context = TranslationContext()
    if "@{" in body:
        resolved_str = resolve_interpolated_string_for_notebook(body, context)
        return f"f{json.dumps(resolved_str)}"
    if not body.startswith("@"):
        return json.dumps(body)

    result = resolve_expression(body, context)
    if result is None:
        return json.dumps(body)
    if result.kind == "notebook_code":
        return result.value
    return json.dumps(result.value)


def _resolve_dict_body(body: dict[str, Any]) -> str:
    """Renders a dict-shaped WebActivity body as Python source.

    Unwraps the ``{"type": "Expression", "value": "@..."}`` shape (the body
    itself is an ADF expression).  Otherwise resolves each value -- when at
    least one value resolves to a DAB ref or interpolation, the result is a
    Python f-string-bearing dict literal so runtime values flow through.
    """
    if body.get("type") == "Expression" and "value" in body:
        return _resolve_body(body["value"])

    context = TranslationContext()
    needs_fstring = False
    resolved: dict[str, Any] = {}
    for key, value in body.items():
        new_value, value_needs_fstring = _resolve_dict_value(value, context)
        resolved[key] = new_value
        needs_fstring = needs_fstring or value_needs_fstring

    if not needs_fstring:
        return json.dumps(resolved)

    parts: list[str] = []
    for key, value in resolved.items():
        if isinstance(value, str) and "{" in value and "dbutils" in value:
            parts.append(f'"{key}": f"{value}"')
        else:
            parts.append(f'"{key}": {json.dumps(value)}')
    return "{" + ", ".join(parts) + "}"


def _resolve_dict_value(value: Any, context: TranslationContext) -> tuple[Any, bool]:
    """Resolves a single dict value; return ``(new_value, needs_fstring)``."""
    if isinstance(value, str) and "@{" in value:
        return resolve_interpolated_string_for_notebook(value, context), True
    if isinstance(value, str) and value.startswith("@"):
        return _resolve_expression_value(value, context, fallback=value)
    if isinstance(value, dict) and value.get("type") == "Expression":
        fallback = value.get("value", str(value))
        return _resolve_expression_value(value, context, fallback=fallback)
    return value, False


def _resolve_expression_value(
    raw: Any, context: TranslationContext, *, fallback: Any
) -> tuple[Any, bool]:
    """Resolves a string/dict expression to either an f-string or a literal.

    Returns ``(value, needs_fstring)``.  ``dab_ref`` results are turned into
    f-string fragments; ``literal`` results pass through; anything else
    falls back to the supplied raw form.
    """
    result = resolve_expression(raw, context)
    if result is None:
        return fallback, False
    if result.kind == "dab_ref":
        return _dab_ref_to_fstring_expr(result.value), True
    if result.kind == "literal":
        return result.value, False
    return fallback, False


def _resolve_headers(headers: dict[str, str] | None) -> tuple[str, str]:
    """Resolves ADF expressions in HTTP header values.

    Header values may be plain strings or ADF expression dicts like
    ``{"type": "Expression", "value": "@concat('Bearer ', ...)"}``.

    Returns:
        A tuple of ``(headers_literal, preamble_code)``:
        - ``headers_literal`` is a Python dict literal for the initial headers
        - ``preamble_code`` is Python code to execute after the headers dict
          is created, adding dynamically computed header values
    """
    if not headers:
        return "{}", ""

    context = TranslationContext()
    static_headers: dict[str, str] = {}
    preamble_lines: list[str] = []

    for key, value in headers.items():
        result = resolve_expression(value, context)
        if result is None:
            static_headers[key] = value if isinstance(value, str) else str(value)
        elif result.kind == "literal":
            static_headers[key] = result.value
        elif result.kind == "dab_ref":
            preamble_lines.append(f'headers["{key}"] = dbutils.widgets.get("{key}")')
        elif result.kind == "notebook_code":
            for import_line in result.imports:
                preamble_lines.insert(0, import_line)
            preamble_lines.append(f'headers["{key}"] = {result.value}')

    headers_literal = json.dumps(static_headers) if static_headers else "{}"
    preamble = ""
    if preamble_lines:
        seen_lines: set[str] = set()
        unique_lines: list[str] = []
        for line in preamble_lines:
            if line not in seen_lines:
                seen_lines.add(line)
                unique_lines.append(line)
        preamble = "\n".join(unique_lines) + "\n"

    return headers_literal, preamble


def _render_sink_write(
    activity: CopyActivity,
    df_var: str = "df",
    *,
    mode: str = "overwrite",
    indent: str = "",
) -> str:
    """Return the Python ``df.write.*`` expression for the activity's sink.

    Honours ``sink_format`` and ``sink_resolved_path`` from the IR so that a
    Copy with a CSV / Parquet / JSON sink writes that format to the resolved
    path instead of always defaulting to a Delta ``saveAsTable``.

    The fallback when no file-format sink is identifiable is still Delta
    ``saveAsTable(target_table)`` — that preserves the existing behaviour for
    sinks the translator can't classify (e.g. unknown ``sink_dataset_type``)
    and matches the Databricks default for managed targets.

    Args:
        activity: The CopyActivity.  Reads ``sink_format``,
            ``sink_resolved_path``, and ``sink_dataset_type``.
        df_var: The Python identifier of the DataFrame to write.
        mode: Spark write mode.  ``overwrite`` for full reads, ``append``
            for incremental ones (e.g. inside ForEach loops).
        indent: Prefix prepended to every emitted line, so the snippet drops
            cleanly into already-indented bodies.

    Returns:
        A multi-line Python snippet.  Trailing newline included.
    """
    fmt = activity.sink_format
    sink_props = activity.sink_properties or {}

    # File-format sink — write the actual format declared by the ADF
    # output dataset.  Delta files written with ``.save(path)`` skip the
    # metastore, which matches the ADF semantic of a path-based dataset.
    if fmt and fmt != "delta":
        opts: list[str] = []
        format_settings = sink_props.get("formatSettings") or {}
        if fmt == "csv":
            if format_settings.get("firstRowAsHeader") is not False:
                opts.append('.option("header", "true")')
            if format_settings.get("columnDelimiter"):
                delim = format_settings["columnDelimiter"]
                opts.append(f'.option("delimiter", "{delim}")')
        opts_str = "".join(opts)

        volume_relative = sink_props.get("volume_relative_path")
        if volume_relative is not None:
            # Volume-rooted sink: ``output_path_root`` is set by the bundler
            # as a base_parameter (with DAB-substituted ``${var.catalog}``
            # / ``${var.schema}``).  Any ``@{...}`` expressions in the
            # ADF dataset's folderPath / fileName have already been
            # rewritten to Python f-string fragments, so we wrap the
            # relative path in an f-string and join.
            rel_literal = volume_relative.replace('"', '\\"')
            preamble = ""
            # Pull in any modules the rewritten f-string fragments reference
            # so the notebook is runnable as-is.  Today the only one is
            # ``datetime`` (from ``@{formatDateTime(...)}`` rewrites).
            if "datetime." in rel_literal:
                preamble = f"{indent}from datetime import datetime\n"
            return (
                f"{preamble}"
                f"{indent}# Volume root is bound by the task's ``output_path_root`` parameter\n"
                f"{indent}# (resolved by DAB to /Volumes/<catalog>/<schema>/<volume>).\n"
                f'{indent}output_path_root = dbutils.widgets.get("output_path_root")\n'
                f'{indent}output_path = f"{{output_path_root}}/{rel_literal}"\n'
                f'{indent}{df_var}.write.format("{fmt}"){opts_str}.mode("{mode}").save(output_path)\n'
            )

        # No structured sink volume — fall back to a single ``output_path``
        # widget the user fills in.  Common when the linked service uses
        # a masked connection string and we can't reconstruct any path.
        return (
            f"{indent}# The ADF output dataset path could not be resolved at translation time.\n"
            f"{indent}# Set ``output_path`` on this task to the destination URI.\n"
            f'{indent}output_path = dbutils.widgets.get("output_path")\n'
            f'{indent}{df_var}.write.format("{fmt}"){opts_str}.mode("{mode}").save(output_path)\n'
        )

    # Delta sink (or unclassified fallback) — keep existing behaviour.
    if fmt == "delta" or fmt is None:
        if mode == "append":
            return (
                f'{indent}{df_var}.write.format("delta").mode("append")'
                f'.option("mergeSchema", "true").saveAsTable(target_table)\n'
            )
        return (
            f'{indent}{df_var}.write.format("delta").mode("{mode}")'
            f'.option("overwriteSchema", "true").saveAsTable(target_table)\n'
        )

    raise ValueError("Invalid fmt string")


def _infer_file_format(source_type: str | None, source_properties: dict | None) -> str:
    """Infer the file format from the source type string and source properties.

    Checks the source_type string first (most reliable), then falls back to
    format settings in source_properties.

    Args:
        source_type: The ADF source type string (e.g. ``"DelimitedTextSource"``).
        source_properties: The source properties dict from the CopyActivity.

    Returns:
        File format string (e.g. ``"csv"``, ``"json"``, ``"parquet"``).
    """
    if source_type:
        type_lower = source_type.lower()
        if "delimitedtext" in type_lower or "csv" in type_lower:
            return "csv"
        if "json" in type_lower:
            return "json"
        if "parquet" in type_lower:
            return "parquet"
        if "avro" in type_lower:
            return "avro"
        if "orc" in type_lower:
            return "orc"

    if source_properties:
        fmt_settings = source_properties.get("formatSettings", {})
        fmt_type = fmt_settings.get("type", "")
        if "Csv" in fmt_type or "Delimited" in fmt_type:
            return "csv"
        if "Json" in fmt_type:
            return "json"
        if "Parquet" in fmt_type:
            return "parquet"
        if "Avro" in fmt_type:
            return "avro"
        if "Orc" in fmt_type:
            return "orc"
        store = source_properties.get("storeSettings", {})
        store_type = store.get("type", "")
        if "BinaryRead" in store_type:
            return "binaryFile"

    return "parquet"


def _generate_autoloader_body(activity: CopyActivity) -> str:
    """Generates Auto Loader ingestion body for file-based sources."""
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    # Prefer the UC volume path when available (set by the copy preparer
    # when an external volume setup task is created); otherwise fall back
    # to the resolved abfss:// path or raw dataset path.
    source_path = source_properties.get(
        "volume_path",
        source_properties.get(
            "resolved_path",
            source_properties.get("path", source_properties.get("filePath", "/mnt/source")),
        ),
    )
    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))
    file_format = _infer_file_format(activity.source_type, source_properties)

    # Use the volume for checkpoints and schema evolution storage instead of
    # /tmp.  This ensures state persists across cluster restarts and is
    # visible in Unity Catalog.
    volume_base = source_properties.get("volume_base", "")
    if volume_base:
        checkpoint = f"{volume_base}/_checkpoints/{activity.task_key}"
    else:
        checkpoint = f"/tmp/checkpoints/{activity.task_key}"

    return textwrap.dedent(f"""\
        # Parameters
        source_path = dbutils.widgets.get("source_path") if dbutils.widgets.get("source_path") else "{source_path}"
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"
        checkpoint_path = "{checkpoint}"

        # Auto Loader: stream file-based source into Delta table
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "{file_format}")
            .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
        )

        # Write to Delta table
        (
            df.writeStream.format("delta")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(target_table)
        )

    """)


def _adf_timeout_to_seconds(value: Any) -> int | None:
    """Parses ADF duration strings (e.g. ``"02:00:00"`` / ``"0.01:30:00"``) to seconds.

    ADF timeout strings use ``[d.]HH:MM:SS`` format.  Return ``None`` when the
    value is empty or can't be parsed so the caller can decide whether to
    omit the option entirely.
    """
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "." in text and text.split(".", 1)[0].isdigit():
        days_part, hms_part = text.split(".", 1)
        days = int(days_part)
    else:
        days = 0
        hms_part = text
    parts = hms_part.split(":")
    if len(parts) != 3:
        return None
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return None
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _generate_jdbc_body(activity: CopyActivity, *, scope: str = "") -> str:
    """Generates JDBC ingestion body for database sources.

    When the SQL query contains an ADF expression like
    ``@concat('SELECT * FROM ', item().schema_name, '.', item().table_name)``,
    the notebook is parameterized to accept an ``item`` widget (passed from
    a ForEach task via ``{{input}}``) and build the query dynamically.
    """
    scope = scope or activity.task_key
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))
    table_name = source_properties.get("tableName", source_properties.get("table", ""))
    query_raw = source_properties.get("sqlReaderQuery", source_properties.get("query", ""))
    query_timeout_seconds = _adf_timeout_to_seconds(source_properties.get("queryTimeout"))
    query_timeout_option = (
        f'\n            .option("queryTimeout", "{query_timeout_seconds}")' if query_timeout_seconds else ""
    )

    # Detect ADF expression dicts that weren't resolved during translation
    query = ""
    is_expression = False
    if isinstance(query_raw, dict) and query_raw.get("type") == "Expression":
        is_expression = True
        query = query_raw.get("value", "")
    elif isinstance(query_raw, str):
        query = query_raw
        if query.startswith("@"):
            is_expression = True

    if is_expression:
        # The query is an ADF expression (e.g. @concat('SELECT * FROM ', item().schema_name, ...)).
        # Generate a notebook that reads the current ForEach item from the
        # "item" widget (set to {{input}} by the for_each_task) and builds
        # the SQL query dynamically.
        return (
            textwrap.dedent(f"""\
            import json

            # Parameters
            default_table = "{sink_table}"
            target_table = dbutils.widgets.get("target_table") or default_table

            # The ForEach task passes the current item as the "item" widget
            # parameter via {{{{input}}}}.  Parse it as JSON to access fields.
            item_raw = dbutils.widgets.get("item")
            item = json.loads(item_raw) if item_raw else {{}}

            # Credentials
            jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc-url")
            jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc-password")
            jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc-user")

            # Build the SQL query from the ForEach item fields.
            # Original ADF expression: {query}
            schema_name = item.get("schema_name", "dbo")
            table_name = item.get("table_name", "UNKNOWN_TABLE")
            query = f"SELECT * FROM {{schema_name}}.{{table_name}}"
            print(f"Executing query: {{query}}")

            # Read from source database via JDBC
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .option("query", query){query_timeout_option}
                .load()
            )

            # Write to the sink defined by the ADF output dataset.  No
            # count/print: those trigger an extra Spark action and can
            # double the read cost.
        """)
            + _render_sink_write(activity, mode="append", indent="")
            + textwrap.dedent("""\

        """)
        )

    if table_name:
        read_option = f'    .option("dbtable", "{table_name}")'
    elif query and "@{" in query:
        # ``@{...}`` interpolation in a SQL query becomes an f-string so
        # ``dbutils.widgets.get(...)`` resolves at runtime.
        resolved_query = resolve_interpolated_string_for_notebook(query, TranslationContext())
        read_option = f'    .option("query", f"""{resolved_query}""")'
    elif query:
        read_option = f'    .option("query", """{query}""")'
    else:
        read_option = '    .option("dbtable", "REPLACE_WITH_TABLE_NAME")'

    return (
        textwrap.dedent(f"""\
        # Parameters
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"

        # Credentials
        jdbc_url = dbutils.secrets.get(scope="{scope}", key="jdbc-url")
        jdbc_password = dbutils.secrets.get(scope="{scope}", key="jdbc-password")
        jdbc_user = dbutils.secrets.get(scope="{scope}", key="jdbc-user")

        # Read from source database via JDBC
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
        {read_option}{query_timeout_option}
            .load()
        )

        # Write to the sink defined by the ADF output dataset.  No count/
        # print: those trigger an extra Spark action and can double the
        # read cost.
    """)
        + _render_sink_write(activity, mode="overwrite", indent="")
        + textwrap.dedent("""\

    """)
    )


def _generate_rest_copy_body(activity: CopyActivity) -> str:
    """Generates REST API ingestion body."""
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    url = source_properties.get("url", source_properties.get("relativeUrl", ""))
    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))

    return (
        textwrap.dedent(f"""\
        import json
        import requests

        # Parameters
        url = dbutils.widgets.get("url") if dbutils.widgets.get("url") else "{url}"
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"
        headers = {{"Content-Type": "application/json"}}

        # Fetch data from REST API
        response = requests.get(url, headers=headers, timeout=300)
        response.raise_for_status()
        data = response.json()

        # Normalize to list of records
        if isinstance(data, dict):
            for key in ("value", "data", "results", "items", "records"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                data = [data]

        # Write to the sink defined by the ADF output dataset.
        df = spark.createDataFrame(data)
    """)
        + _render_sink_write(activity, mode="overwrite", indent="")
        + textwrap.dedent("""\

    """)
    )


def _generate_generic_copy_body(activity: CopyActivity) -> str:
    """Generates a generic Spark read/write copy body as fallback."""
    source_properties = activity.source_properties or {}
    sink_properties = activity.sink_properties or {}

    # Use the resolved path from dataset if available, otherwise fall back
    source_path = source_properties.get(
        "resolved_path",
        source_properties.get("path", source_properties.get("filePath", "/mnt/source")),
    )
    sink_table = sink_properties.get("table", sink_properties.get("tableName", f"{activity.task_key}_raw"))
    file_format = _infer_file_format(activity.source_type, source_properties)

    return (
        textwrap.dedent(f"""\
        # Parameters
        source_path = dbutils.widgets.get("source_path") if dbutils.widgets.get("source_path") else "{source_path}"
        target_table = dbutils.widgets.get("target_table") if dbutils.widgets.get("target_table") else "{sink_table}"

        # Read source data
        df = spark.read.format("{file_format}").load(source_path)

        # Write to the sink defined by the ADF output dataset.
    """)
        + _render_sink_write(activity, mode="overwrite", indent="")
        + textwrap.dedent("""\

    """)
    )
