"""Unit tests for the unified resolve_expression() function."""

from __future__ import annotations

from types import MappingProxyType

from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import (
    parse_expression,
    parse_expression_for_dab,
    resolve_expression,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(**variable_mappings: str) -> TranslationContext:
    """Build a context with optional variable -> task_key mappings."""
    vc = MappingProxyType(variable_mappings) if variable_mappings else MappingProxyType({})
    return TranslationContext(variable_cache=vc)


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------


class TestLiterals:
    def test_plain_string(self):
        r = resolve_expression("hello", _ctx())
        assert r is not None
        assert r.kind == "literal"
        assert r.value == "hello"

    def test_integer(self):
        r = resolve_expression(42, _ctx())
        assert r is not None
        assert r.kind == "literal"
        assert r.value == "42"

    def test_float(self):
        r = resolve_expression(3.14, _ctx())
        assert r is not None
        assert r.kind == "literal"
        assert r.value == "3.14"

    def test_boolean(self):
        r = resolve_expression(True, _ctx())
        assert r is not None
        assert r.kind == "literal"
        assert r.value == "True"

    def test_expression_dict_wrapping(self):
        r = resolve_expression({"type": "Expression", "value": "hello"}, _ctx())
        assert r is not None
        assert r.kind == "literal"
        assert r.value == "hello"


# ---------------------------------------------------------------------------
# DAB refs: pipeline properties
# ---------------------------------------------------------------------------


class TestPipelineProperties:
    def test_run_id(self):
        r = resolve_expression("@pipeline().RunId", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{job.run_id}}"

    def test_pipeline_name(self):
        r = resolve_expression("@pipeline().Pipeline", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{job.name}}"

    def test_trigger_time(self):
        r = resolve_expression("@pipeline().TriggerTime", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{job.start_time.iso_datetime}}"

    def test_group_id(self):
        r = resolve_expression("@pipeline().GroupId", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{job.run_id}}"


# ---------------------------------------------------------------------------
# DAB refs: pipeline parameters
# ---------------------------------------------------------------------------


class TestPipelineParameters:
    def test_parameter(self):
        r = resolve_expression("@pipeline().parameters.environment", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{job.parameters.environment}}"

    def test_parameter_expression_dict(self):
        r = resolve_expression(
            {"type": "Expression", "value": "@pipeline().parameters.date"},
            _ctx(),
        )
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{job.parameters.date}}"


# ---------------------------------------------------------------------------
# DAB refs: activity output
# ---------------------------------------------------------------------------


class TestActivityOutput:
    def test_firstrow_column(self):
        r = resolve_expression("@activity('Lookup').output.firstRow.cnt", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{tasks.Lookup.values.cnt}}"

    def test_output_value(self):
        r = resolve_expression("@activity('GetList').output.value", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{tasks.GetList.values.result}}"

    def test_output_no_path(self):
        r = resolve_expression("@activity('Task').output", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{tasks.Task.values.result}}"

    def test_sanitizes_task_key(self):
        r = resolve_expression("@activity('Lookup Row Count').output.firstRow.row_count", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert "Lookup_Row_Count" in r.value


# ---------------------------------------------------------------------------
# DAB refs: variables
# ---------------------------------------------------------------------------


class TestVariables:
    def test_variable_with_context(self):
        r = resolve_expression("@variables('outputPath')", _ctx(outputPath="SetOutputPath"))
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{tasks.SetOutputPath.values.outputPath}}"

    def test_variable_with_explicit_task_keys(self):
        r = resolve_expression(
            "@variables('runDate')",
            _ctx(),
            variable_task_keys={"runDate": "SetRunDate"},
        )
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{tasks.SetRunDate.values.runDate}}"

    def test_variable_fallback_to_name(self):
        r = resolve_expression("@variables('unknown')", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{tasks.unknown.values.unknown}}"


# ---------------------------------------------------------------------------
# DAB refs: item()
# ---------------------------------------------------------------------------


class TestItem:
    def test_item(self):
        r = resolve_expression("@item()", _ctx())
        assert r is not None
        assert r.kind == "dab_ref"
        assert r.value == "{{input}}"


# ---------------------------------------------------------------------------
# Notebook code: utcNow
# ---------------------------------------------------------------------------


class TestUtcNow:
    def test_utcnow_no_format(self):
        r = resolve_expression("@utcNow()", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "isoformat" in r.value
        assert "from datetime import datetime, timezone" in r.imports

    def test_utcnow_with_format(self):
        r = resolve_expression("@utcNow('yyyy-MM-dd')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "strftime" in r.value
        assert "%Y-%m-%d" in r.value

    def test_utcnow_expression_dict(self):
        r = resolve_expression(
            {"type": "Expression", "value": "@utcNow('yyyy-MM-dd')"},
            _ctx(),
        )
        assert r is not None
        assert r.kind == "notebook_code"


# ---------------------------------------------------------------------------
# Notebook code: concat
# ---------------------------------------------------------------------------


class TestConcat:
    def test_concat_literals(self):
        r = resolve_expression("@concat('hello', ' ', 'world')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        # Should produce a Python concatenation
        assert "+" in r.value

    def test_concat_with_variable(self):
        r = resolve_expression(
            "@concat('output/', variables('runDate'), '/processed')",
            _ctx(runDate="SetRunDate"),
        )
        assert r is not None
        assert r.kind == "notebook_code"
        assert "runDate" in r.value

    def test_concat_with_utcnow(self):
        r = resolve_expression("@concat('date_', utcNow('yyyy-MM-dd'))", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "strftime" in r.value

    def test_concat_with_pipeline_param(self):
        r = resolve_expression(
            "@concat(variables('catalogName'), '.', pipeline().parameters.schemaPrefix, '_data')",
            _ctx(catalogName="SetCatalogName"),
        )
        assert r is not None
        assert r.kind == "notebook_code"


# ---------------------------------------------------------------------------
# Unsupported expressions
# ---------------------------------------------------------------------------


class TestUnsupported:
    def test_non_dict_non_scalar(self):
        r = resolve_expression({"type": "Other"}, _ctx())
        assert r is None

    def test_complex_expression(self):
        r = resolve_expression("@if(equals(1,1),'yes','no')", _ctx())
        assert r is None


# ---------------------------------------------------------------------------
# Backward-compat wrappers
# ---------------------------------------------------------------------------


class TestBackwardCompat:
    def test_parse_expression_returns_value(self):
        result = parse_expression("@pipeline().RunId", _ctx())
        assert result == "{{job.run_id}}"

    def test_parse_expression_returns_none_for_unsupported(self):
        result = parse_expression("@if(1,2,3)", _ctx())
        assert result is None

    def test_parse_expression_for_dab_returns_ref(self):
        result = parse_expression_for_dab("@pipeline().RunId")
        assert result == "{{job.run_id}}"

    def test_parse_expression_for_dab_returns_none_for_notebook_code(self):
        result = parse_expression_for_dab("@utcNow()")
        assert result is None

    def test_parse_expression_for_dab_returns_none_for_non_expression(self):
        result = parse_expression_for_dab("plain_string")
        assert result is None
