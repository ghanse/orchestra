"""Shared helpers used by every activity preparer.

The per-activity preparer modules used to duplicate three concerns:

1. *Resolving an ADF parameter value* (handling ``@expr`` and ``@{...}``
   interpolation forms uniformly).
2. *Building the standard notebook-task scaffolding* -- attaching a
   ``notebook_task`` dict, computing the bundle-relative path, and
   producing the matching :class:`DabNotebook` artifact.
3. *Emitting JDBC credential ``SecretInstruction`` rows* for source
   types that read with ``spark.read.format("jdbc")``.

Centralising them here keeps the per-activity modules focused on
activity-specific shape rather than rebuilding the same scaffolding.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.models.dab import DabNotebook, SecretInstruction
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string

if TYPE_CHECKING:
    pass


def resolve_param_value(value: str) -> str:
    """Resolve an ADF expression in a parameter value to its DAB form.

    ADF parameter values may carry three shapes:

    * ``@func(...)`` -- a top-level expression call.
    * ``"prefix-@{func(...)}-suffix"`` -- ``@{...}`` interpolated into a
      string literal.
    * a plain literal -- returned untouched.

    Only ``"dab_ref"`` and ``"literal"`` results are folded back into
    the value.  ``"notebook_code"`` results would emit Python source into
    a base_parameter (which DAB cannot evaluate), so the original raw
    expression is returned and the caller is expected to surface it via
    the manual-parameter handling path.
    """
    if "@{" in value:
        return resolve_interpolated_string(value, TranslationContext())
    if value.startswith("@"):
        result = resolve_expression(value, TranslationContext())
        if result is not None and result.kind in ("dab_ref", "literal"):
            return result.value
    return value


def build_notebook_task_artifacts(
    *,
    notebook_relative_path: str,
    notebook_content: str,
    base_parameters: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[DabNotebook]]:
    """Build the ``notebook_task`` dict and the matching DabNotebook artifact.

    The bundle layout always places generated notebooks under
    ``src/<notebook_relative_path>`` and the job YAML refers to them via
    ``../src/<notebook_relative_path>``.  This helper constructs both
    halves so a preparer just hands in its body content and parameters.

    Args:
        notebook_relative_path: Path relative to ``src/`` (e.g.
            ``notebooks/copy_orders.py``).
        notebook_content: Full notebook source text.
        base_parameters: Optional base_parameters for the notebook_task.

    Returns:
        A tuple of (notebook_task dict, [single DabNotebook]).
    """
    notebook_task: dict[str, Any] = {
        "notebook_path": f"../src/{notebook_relative_path}",
    }
    if base_parameters is not None:
        notebook_task["base_parameters"] = base_parameters

    notebooks = [
        DabNotebook(
            relative_path=notebook_relative_path,
            content=notebook_content,
        )
    ]
    return notebook_task, notebooks


def make_jdbc_secrets(
    *,
    scope_name: str,
    source_type: str,
    activity_name: str,
    role: str = "source",
) -> list[SecretInstruction]:
    """Return the standard ``jdbc-url`` / ``jdbc-password`` secret pair.

    Activities that read from a JDBC-style connector (Lookup, Copy with
    a database source) all need the same credential pair under the same
    scope.  ``role`` differentiates the value-source descriptions in
    SETUP.md (e.g. "JDBC URL for AzureSqlSource lookup in activity 'Foo'"
    vs "JDBC URL for AzureSqlSource source in activity 'Foo'").
    """
    return [
        SecretInstruction(
            scope=scope_name,
            key="jdbc-url",
            value_source=f"JDBC URL for {source_type} {role} in activity '{activity_name}'",
        ),
        SecretInstruction(
            scope=scope_name,
            key="jdbc-password",
            value_source=f"JDBC password for {source_type} {role} in activity '{activity_name}'",
        ),
    ]
