"""Shared helpers used by every activity preparer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orchestra.models.dab import DabNotebook, SecretInstruction
from orchestra.models.ir import TranslationContext
from orchestra.parser.expression_parser import resolve_expression, resolve_interpolated_string
from orchestra.preparer.workflow_preparer import build_common_task_fields

if TYPE_CHECKING:
    from orchestra.models.ir import Activity


def resolve_param_value(value: str) -> str:
    """Resolves an ADF expression in a parameter value to its DAB form.

    Only ``literal`` and ``dab_ref`` results are folded back into the value.
    ``notebook_code`` results would emit Python source into a base_parameter
    (which DAB cannot evaluate), so the raw expression is returned and the
    caller is expected to surface it via the manual-parameter handling path.
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
    """Builds the ``notebook_task`` dict and the matching DabNotebook artifact.

    The bundle layout places generated notebooks under
    ``src/<notebook_relative_path>`` and the job YAML refers to them via
    ``../src/<notebook_relative_path>``.
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


def build_notebook_activity_task(
    activity: Activity,
    *,
    notebook_relative_path: str,
    notebook_content: str,
    base_parameters: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[DabNotebook]]:
    """Builds the common task fields and notebook_task scaffolding for *activity*.

    Returns ``(task, notebooks)`` where ``task`` already has ``notebook_task``
    attached and ``notebooks`` contains the single :class:`DabNotebook`
    artifact for the generated body.
    """
    task = build_common_task_fields(activity)
    notebook_task, notebooks = build_notebook_task_artifacts(
        notebook_relative_path=notebook_relative_path,
        notebook_content=notebook_content,
        base_parameters=base_parameters,
    )
    task["notebook_task"] = notebook_task
    return task, notebooks


def make_jdbc_secrets(
    *,
    scope_name: str,
    source_type: str,
    activity_name: str,
    role: str = "source",
) -> list[SecretInstruction]:
    """Returns the ``jdbc-url`` / ``jdbc-password`` secret pair for a JDBC connector.

    ``role`` differentiates the value-source description in SETUP.md
    (``"source"``, ``"sink"``, ``"lookup"``).
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
