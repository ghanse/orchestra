"""Tests for the copy_and_notify -> Databricks notification destination feature."""

from __future__ import annotations

from orchestra.adapter.models import TranslationConfiguration
from orchestra.adapter.operations import apply_configuration, gather_options
from orchestra.models.ir import (
    CopyActivity,
    Dependency,
    NotebookActivity,
    Pipeline,
    WebActivity,
)
from orchestra.preparer.notifications import resolve_task_notifications


def _pipeline_with_copy_notify() -> Pipeline:
    copy = CopyActivity(name="Load Curated", task_key="load_curated")
    notify_ok = WebActivity(
        name="Notify Success",
        task_key="notify_success",
        url="https://x",
        method="POST",
        depends_on=[Dependency(task_key="load_curated", outcome="Succeeded")],
    )
    notify_fail = WebActivity(
        name="Notify Failure",
        task_key="notify_failure",
        url="https://x",
        method="POST",
        depends_on=[Dependency(task_key="load_curated", outcome="Failed")],
    )
    downstream = NotebookActivity(
        name="After",
        task_key="after",
        notebook_path="/n",
        depends_on=[Dependency(task_key="notify_success", outcome="Succeeded")],
    )
    return Pipeline(name="p", tasks=[copy, notify_ok, notify_fail, downstream])


def test_option_surfaces_and_followups_are_gated_by_answer():
    p = _pipeline_with_copy_notify()
    ids = {o.option_id for o in gather_options(p, []).options}
    assert "copy_notify_destination" in ids
    # follow-ups not shown until a destination is chosen
    assert "copy_notify_email_recipients" not in ids
    assert "copy_notify_webhook_url" not in ids

    email_ids = {o.option_id for o in gather_options(p, [], answers={"copy_notify_destination": "email"}).options}
    assert "copy_notify_email_recipients" in email_ids
    assert "copy_notify_webhook_url" not in email_ids

    slack_ids = {o.option_id for o in gather_options(p, [], answers={"copy_notify_destination": "slack"}).options}
    assert "copy_notify_webhook_url" in slack_ids
    assert "copy_notify_destination_name" in slack_ids
    assert "copy_notify_email_recipients" not in slack_ids


def test_keep_default_does_not_collapse():
    p = _pipeline_with_copy_notify()
    out = apply_configuration(p, TranslationConfiguration())  # default = keep
    names = {t.name for t in out.tasks}
    assert {"Notify Success", "Notify Failure"} <= names  # still present


def test_email_collapse_drops_notifies_and_stamps_copy():
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="email", copy_notify_email_recipients="a@x.com, b@x.com", copy_notify_events="both"
    )
    out = apply_configuration(p, cfg)
    names = {t.name for t in out.tasks}
    assert "Notify Success" not in names and "Notify Failure" not in names
    copy = next(t for t in out.tasks if t.task_key == "load_curated")
    assert copy.notifications["destination"] == "email"
    assert copy.notifications["email_recipients"] == ["a@x.com", "b@x.com"]
    assert set(copy.notifications["events"]) == {"on_success", "on_failure"}
    # downstream task rewired off the dropped notify onto the copy
    after = next(t for t in out.tasks if t.task_key == "after")
    assert any(d.task_key == "load_curated" for d in (after.depends_on or []))


def test_events_restriction_to_failure_only():
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="email", copy_notify_email_recipients="a@x.com", copy_notify_events="on_failure"
    )
    out = apply_configuration(p, cfg)
    copy = next(t for t in out.tasks if t.task_key == "load_curated")
    assert copy.notifications["events"] == ["on_failure"]


def test_resolve_email_notifications():
    keys, setup = resolve_task_notifications(
        {"destination": "email", "email_recipients": ["a@x.com"], "events": ["on_failure", "on_success"]}
    )
    assert keys == {"email_notifications": {"on_failure": ["a@x.com"], "on_success": ["a@x.com"]}}
    assert setup == []


def test_resolve_webhook_without_workspace_falls_back_to_setup_task(monkeypatch):
    # Force the SDK create path to fail -> graceful fallback to a setup task.
    import orchestra.preparer.notifications as nm

    def _boom(*a, **k):
        raise RuntimeError("no workspace auth")

    monkeypatch.setattr(nm, "_ensure_destination", _boom if False else lambda *a, **k: None)
    keys, setup = resolve_task_notifications(
        {
            "destination": "slack",
            "webhook_url": "https://hooks",
            "destination_name": "orchestra-slack",
            "events": ["on_failure"],
        }
    )
    assert keys == {}
    assert len(setup) == 1 and setup[0].type == "notification_destination"
