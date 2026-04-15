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
        assert r.kind == "dab_ref"
        assert r.value == "{{job.start_time.iso_datetime}}"

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

    def test_agentic_data_uri(self):
        r = resolve_expression("@dataUri('hello')", _ctx())
        assert r is None

    def test_agentic_xml(self):
        r = resolve_expression("@xml('<root/>')", _ctx())
        assert r is None

    def test_agentic_xpath(self):
        r = resolve_expression("@xpath(xml('<r/>'), '/')", _ctx())
        assert r is None

    def test_agentic_convert_from_utc(self):
        r = resolve_expression("@convertFromUtc('2024-01-01T00:00:00Z', 'Pacific Standard Time')", _ctx())
        assert r is None

    def test_agentic_ticks(self):
        r = resolve_expression("@ticks('2024-01-01T00:00:00Z')", _ctx())
        assert r is None


# ---------------------------------------------------------------------------
# String functions
# ---------------------------------------------------------------------------


class TestStringFunctions:
    def test_ends_with(self):
        r = resolve_expression("@endsWith('hello world', 'world')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "endswith" in r.value

    def test_ends_with_nested(self):
        r = resolve_expression("@endsWith(toLower('HELLO'), 'hello')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "endswith" in r.value
        assert "lower" in r.value

    def test_guid_no_args(self):
        r = resolve_expression("@guid()", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "uuid4" in r.value

    def test_guid_format_n(self):
        r = resolve_expression("@guid('N')", _ctx())
        assert r is not None
        assert "replace" in r.value

    def test_index_of(self):
        r = resolve_expression("@indexOf('hello world', 'world')", _ctx())
        assert r is not None
        assert "find" in r.value

    def test_last_index_of(self):
        r = resolve_expression("@lastIndexOf('hello hello', 'hello')", _ctx())
        assert r is not None
        assert "rfind" in r.value

    def test_replace(self):
        r = resolve_expression("@replace('hello world', 'world', 'python')", _ctx())
        assert r is not None
        assert "replace" in r.value

    def test_split(self):
        r = resolve_expression("@split('a,b,c', ',')", _ctx())
        assert r is not None
        assert "split" in r.value

    def test_starts_with(self):
        r = resolve_expression("@startsWith('hello world', 'hello')", _ctx())
        assert r is not None
        assert "startswith" in r.value

    def test_substring(self):
        r = resolve_expression("@substring('hello', 0, 3)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        # Should produce a slice expression
        assert "[" in r.value

    def test_to_lower(self):
        r = resolve_expression("@toLower('HELLO')", _ctx())
        assert r is not None
        assert "lower" in r.value

    def test_to_upper(self):
        r = resolve_expression("@toUpper('hello')", _ctx())
        assert r is not None
        assert "upper" in r.value

    def test_trim(self):
        r = resolve_expression("@trim('  hello  ')", _ctx())
        assert r is not None
        assert "strip" in r.value


# ---------------------------------------------------------------------------
# Collection functions
# ---------------------------------------------------------------------------


class TestCollectionFunctions:
    def test_contains(self):
        r = resolve_expression("@contains('hello world', 'hello')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "in" in r.value

    def test_empty(self):
        r = resolve_expression("@empty('')", _ctx())
        assert r is not None
        assert "len" in r.value

    def test_first(self):
        r = resolve_expression("@first(createArray(1, 2, 3))", _ctx())
        assert r is not None
        assert "[0]" in r.value

    def test_join(self):
        r = resolve_expression("@join(createArray('a', 'b', 'c'), ',')", _ctx())
        assert r is not None
        assert "join" in r.value

    def test_last(self):
        r = resolve_expression("@last(createArray(1, 2, 3))", _ctx())
        assert r is not None
        assert "[-1]" in r.value

    def test_length(self):
        r = resolve_expression("@length('hello')", _ctx())
        assert r is not None
        assert "len" in r.value

    def test_skip(self):
        r = resolve_expression("@skip(createArray(1, 2, 3), 1)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"

    def test_take(self):
        r = resolve_expression("@take(createArray(1, 2, 3), 2)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"

    def test_intersection(self):
        r = resolve_expression("@intersection(createArray(1, 2, 3), createArray(2, 3, 4))", _ctx())
        assert r is not None
        assert "set" in r.value

    def test_union(self):
        r = resolve_expression("@union(createArray(1, 2), createArray(3, 4))", _ctx())
        assert r is not None
        assert "set" in r.value


# ---------------------------------------------------------------------------
# Logical functions
# ---------------------------------------------------------------------------


class TestLogicalFunctions:
    def test_and(self):
        r = resolve_expression("@and(true, false)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "and" in r.value

    def test_equals(self):
        r = resolve_expression("@equals(1, 1)", _ctx())
        assert r is not None
        assert "==" in r.value

    def test_greater(self):
        r = resolve_expression("@greater(5, 3)", _ctx())
        assert r is not None
        assert ">" in r.value

    def test_greater_or_equals(self):
        r = resolve_expression("@greaterOrEquals(5, 5)", _ctx())
        assert r is not None
        assert ">=" in r.value

    def test_if(self):
        r = resolve_expression("@if(equals(1, 1), 'yes', 'no')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "if" in r.value
        assert "else" in r.value

    def test_less(self):
        r = resolve_expression("@less(3, 5)", _ctx())
        assert r is not None
        assert "<" in r.value

    def test_less_or_equals(self):
        r = resolve_expression("@lessOrEquals(3, 3)", _ctx())
        assert r is not None
        assert "<=" in r.value

    def test_not(self):
        r = resolve_expression("@not(true)", _ctx())
        assert r is not None
        assert "not" in r.value

    def test_or(self):
        r = resolve_expression("@or(true, false)", _ctx())
        assert r is not None
        assert "or" in r.value


# ---------------------------------------------------------------------------
# Conversion functions
# ---------------------------------------------------------------------------


class TestConversionFunctions:
    def test_array(self):
        r = resolve_expression("@array('hello')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "[" in r.value

    def test_base64(self):
        r = resolve_expression("@base64('hello')", _ctx())
        assert r is not None
        assert "b64encode" in r.value

    def test_base64_to_string(self):
        r = resolve_expression("@base64ToString('aGVsbG8=')", _ctx())
        assert r is not None
        assert "b64decode" in r.value
        assert "decode" in r.value

    def test_base64_to_binary(self):
        r = resolve_expression("@base64ToBinary('aGVsbG8=')", _ctx())
        assert r is not None
        assert "b64decode" in r.value

    def test_binary(self):
        r = resolve_expression("@binary('hello')", _ctx())
        assert r is not None
        assert "encode" in r.value

    def test_bool(self):
        r = resolve_expression("@bool(1)", _ctx())
        assert r is not None
        assert "bool" in r.value

    def test_coalesce(self):
        r = resolve_expression("@coalesce(null, 'fallback')", _ctx())
        assert r is not None
        assert "next" in r.value
        assert "None" in r.value

    def test_create_array(self):
        r = resolve_expression("@createArray(1, 2, 3)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "[" in r.value

    def test_decode_base64_alias(self):
        r = resolve_expression("@decodeBase64('aGVsbG8=')", _ctx())
        assert r is not None
        assert "b64decode" in r.value

    def test_decode_uri_component(self):
        r = resolve_expression("@decodeUriComponent('hello%20world')", _ctx())
        assert r is not None
        assert "unquote" in r.value

    def test_encode_uri_component(self):
        r = resolve_expression("@encodeUriComponent('hello world')", _ctx())
        assert r is not None
        assert "quote" in r.value

    def test_float(self):
        r = resolve_expression("@float('3.14')", _ctx())
        assert r is not None
        assert "float" in r.value

    def test_int(self):
        r = resolve_expression("@int('42')", _ctx())
        assert r is not None
        assert "int" in r.value

    def test_json(self):
        r = resolve_expression('@json(\'{"key": "value"}\')', _ctx())
        assert r is not None
        assert "loads" in r.value

    def test_string(self):
        r = resolve_expression("@string(42)", _ctx())
        assert r is not None
        assert "str" in r.value

    def test_uri_component_alias(self):
        r = resolve_expression("@uriComponent('hello world')", _ctx())
        assert r is not None
        assert "quote" in r.value

    def test_uri_component_to_string_alias(self):
        r = resolve_expression("@uriComponentToString('hello%20world')", _ctx())
        assert r is not None
        assert "unquote" in r.value


# ---------------------------------------------------------------------------
# Math functions
# ---------------------------------------------------------------------------


class TestMathFunctions:
    def test_add(self):
        r = resolve_expression("@add(1, 2)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "+" in r.value

    def test_div(self):
        r = resolve_expression("@div(10, 3)", _ctx())
        assert r is not None
        assert "//" in r.value

    def test_max(self):
        r = resolve_expression("@max(1, 5, 3)", _ctx())
        assert r is not None
        assert "max" in r.value

    def test_min(self):
        r = resolve_expression("@min(1, 5, 3)", _ctx())
        assert r is not None
        assert "min" in r.value

    def test_mod(self):
        r = resolve_expression("@mod(7, 3)", _ctx())
        assert r is not None
        assert "%" in r.value

    def test_mul(self):
        r = resolve_expression("@mul(3, 4)", _ctx())
        assert r is not None
        assert "*" in r.value

    def test_rand(self):
        r = resolve_expression("@rand(1, 100)", _ctx())
        assert r is not None
        assert "randint" in r.value

    def test_range(self):
        r = resolve_expression("@range(0, 10)", _ctx())
        assert r is not None
        assert "range" in r.value

    def test_sub(self):
        r = resolve_expression("@sub(10, 3)", _ctx())
        assert r is not None
        assert "-" in r.value


# ---------------------------------------------------------------------------
# Date/time functions
# ---------------------------------------------------------------------------


class TestDateTimeFunctions:
    def test_add_days(self):
        r = resolve_expression("@addDays('2024-01-01T00:00:00', 5)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "timedelta" in r.value
        assert "days" in r.value

    def test_add_days_with_format(self):
        r = resolve_expression("@addDays('2024-01-01T00:00:00', 5, 'yyyy-MM-dd')", _ctx())
        assert r is not None
        assert "strftime" in r.value
        assert "%Y-%m-%d" in r.value

    def test_add_hours(self):
        r = resolve_expression("@addHours('2024-01-01T00:00:00', 3)", _ctx())
        assert r is not None
        assert "hours" in r.value

    def test_add_minutes(self):
        r = resolve_expression("@addMinutes('2024-01-01T00:00:00', 30)", _ctx())
        assert r is not None
        assert "minutes" in r.value

    def test_add_seconds(self):
        r = resolve_expression("@addSeconds('2024-01-01T00:00:00', 90)", _ctx())
        assert r is not None
        assert "seconds" in r.value

    def test_add_to_time(self):
        r = resolve_expression("@addToTime('2024-01-01T00:00:00', 2, 'Hour')", _ctx())
        assert r is not None
        assert "timedelta" in r.value
        assert "hours" in r.value

    def test_day_of_month(self):
        r = resolve_expression("@dayOfMonth('2024-01-15T00:00:00')", _ctx())
        assert r is not None
        assert ".day" in r.value

    def test_day_of_week(self):
        r = resolve_expression("@dayOfWeek('2024-01-15T00:00:00')", _ctx())
        assert r is not None
        assert "isoweekday" in r.value

    def test_day_of_year(self):
        r = resolve_expression("@dayOfYear('2024-01-15T00:00:00')", _ctx())
        assert r is not None
        assert "tm_yday" in r.value

    def test_format_date_time(self):
        r = resolve_expression("@formatDateTime('2024-01-01T00:00:00', 'yyyy-MM-dd')", _ctx())
        assert r is not None
        assert "strftime" in r.value
        assert "%Y-%m-%d" in r.value

    def test_format_date_time_no_format(self):
        r = resolve_expression("@formatDateTime('2024-01-01T00:00:00')", _ctx())
        assert r is not None
        assert "isoformat" in r.value

    def test_get_future_time(self):
        r = resolve_expression("@getFutureTime(5, 'Day')", _ctx())
        assert r is not None
        assert "timedelta" in r.value
        assert "days" in r.value
        assert "from datetime import datetime, timezone, timedelta" in r.imports

    def test_get_past_time(self):
        r = resolve_expression("@getPastTime(3, 'Hour')", _ctx())
        assert r is not None
        assert "timedelta" in r.value
        assert "hours" in r.value

    def test_start_of_day(self):
        r = resolve_expression("@startOfDay('2024-01-15T14:30:00')", _ctx())
        assert r is not None
        assert "hour=0" in r.value

    def test_start_of_hour(self):
        r = resolve_expression("@startOfHour('2024-01-15T14:30:00')", _ctx())
        assert r is not None
        assert "minute=0" in r.value

    def test_start_of_month(self):
        r = resolve_expression("@startOfMonth('2024-01-15T14:30:00')", _ctx())
        assert r is not None
        assert "day=1" in r.value

    def test_subtract_from_time(self):
        r = resolve_expression("@subtractFromTime('2024-01-15T00:00:00', 5, 'Day')", _ctx())
        assert r is not None
        assert "timedelta" in r.value
        assert " - " in r.value


# ---------------------------------------------------------------------------
# Nested function calls
# ---------------------------------------------------------------------------


class TestNestedFunctions:
    def test_concat_with_toLower_and_toUpper(self):
        r = resolve_expression("@concat(toLower('Hello'), toUpper('world'))", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "lower" in r.value
        assert "upper" in r.value

    def test_if_with_equals(self):
        r = resolve_expression("@if(equals(1, 1), 'yes', 'no')", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "==" in r.value
        assert "if" in r.value

    def test_deeply_nested(self):
        r = resolve_expression("@concat(toLower(trim(' HELLO ')), '_', toUpper('world'))", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "lower" in r.value
        assert "strip" in r.value
        assert "upper" in r.value

    def test_first_of_create_array(self):
        r = resolve_expression("@first(createArray('a', 'b', 'c'))", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "[0]" in r.value

    def test_length_of_split(self):
        r = resolve_expression("@length(split('a,b,c', ','))", _ctx())
        assert r is not None
        assert "len" in r.value
        assert "split" in r.value

    def test_nested_math(self):
        r = resolve_expression("@add(mul(2, 3), sub(10, 4))", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"

    def test_replace_with_pipeline_param(self):
        r = resolve_expression(
            "@replace(pipeline().parameters.path, '/old/', '/new/')",
            _ctx(),
        )
        assert r is not None
        assert r.kind == "notebook_code"
        assert "replace" in r.value
        assert "dbutils.widgets.get" in r.value


# ---------------------------------------------------------------------------
# Functions with DAB ref arguments
# ---------------------------------------------------------------------------


class TestFunctionsWithDabRefs:
    def test_to_lower_with_pipeline_param(self):
        r = resolve_expression("@toLower(pipeline().parameters.env)", _ctx())
        assert r is not None
        assert r.kind == "notebook_code"
        assert "lower" in r.value
        assert "dbutils.widgets.get" in r.value

    def test_concat_with_variable_and_literal(self):
        r = resolve_expression(
            "@concat(variables('prefix'), '_suffix')",
            _ctx(prefix="SetPrefix"),
        )
        assert r is not None
        assert r.kind == "notebook_code"
        assert "dbutils.widgets.get" in r.value

    def test_equals_with_activity_output(self):
        r = resolve_expression(
            "@equals(activity('Check').output.firstRow.status, 'done')",
            _ctx(),
        )
        assert r is not None
        assert r.kind == "notebook_code"
        assert "==" in r.value
        assert "dbutils.widgets.get" in r.value


# ---------------------------------------------------------------------------
# Backward-compat wrappers
# ---------------------------------------------------------------------------


class TestBackwardCompat:
    def test_parse_expression_returns_value(self):
        result = parse_expression("@pipeline().RunId", _ctx())
        assert result == "{{job.run_id}}"

    def test_parse_expression_returns_none_for_unsupported(self):
        result = parse_expression("@dataUri('hello')", _ctx())
        assert result is None

    def test_parse_expression_for_dab_returns_ref(self):
        result = parse_expression_for_dab("@pipeline().RunId")
        assert result == "{{job.run_id}}"

    def test_parse_expression_for_dab_returns_ref_for_utcnow(self):
        result = parse_expression_for_dab("@utcNow()")
        assert result == "{{job.start_time.iso_datetime}}"

    def test_parse_expression_for_dab_returns_none_for_non_expression(self):
        result = parse_expression_for_dab("plain_string")
        assert result is None
