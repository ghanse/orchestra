"""Tests for the copy_and_notify -> Databricks notification destination feature."""

from __future__ import annotations

from orchestra.adapter.models import TranslationConfiguration
from orchestra.adapter.operations import (
    apply_configuration,
    collect_copy_notify_args,
    gather_options,
    provision_notification_destinations,
)
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
    assert "copy_notify_slack_url" not in ids

    email_ids = {o.option_id for o in gather_options(p, [], answers={"copy_notify_destination": "email"}).options}
    assert "copy_notify_email_recipients" in email_ids
    assert "copy_notify_slack_url" not in email_ids

    slack_ids = {o.option_id for o in gather_options(p, [], answers={"copy_notify_destination": "slack"}).options}
    assert "copy_notify_slack_url" in slack_ids
    assert "copy_notify_destination_name" in slack_ids
    assert "copy_notify_email_recipients" not in slack_ids


def test_chain_surfaces_every_sdk_field_for_destination():
    """Each SDK field of the chosen destination becomes its own follow-up option,
    in registry order (required first), so the agent can prompt sequentially."""
    p = _pipeline_with_copy_notify()
    webhook_ids = [o.option_id for o in gather_options(p, [], answers={"copy_notify_destination": "webhook"}).options]
    # SDK fields surface in registry order (required url first), then name + events
    webhook_fields = [i for i in webhook_ids if i.startswith("copy_notify_webhook")]
    assert webhook_fields == [
        "copy_notify_webhook_url",
        "copy_notify_webhook_username",
        "copy_notify_webhook_password",
    ]
    assert "copy_notify_destination_name" in webhook_ids
    assert "copy_notify_events" in webhook_ids

    slack_ids = [o.option_id for o in gather_options(p, [], answers={"copy_notify_destination": "slack"}).options]
    slack_fields = [i for i in slack_ids if i.startswith("copy_notify_slack")]
    assert slack_fields == [
        "copy_notify_slack_url",
        "copy_notify_slack_channel_id",
        "copy_notify_slack_oauth_token",
    ]


def test_answered_field_drops_out_of_the_chain():
    """Already-answered follow-ups are filtered, so the chain advances field by field."""
    p = _pipeline_with_copy_notify()
    answers = {"copy_notify_destination": "slack", "copy_notify_slack_url": "https://hooks.slack.com/x"}
    ids = {o.option_id for o in gather_options(p, [], answers=answers).options}
    assert "copy_notify_slack_url" not in ids  # answered -> gone
    assert "copy_notify_slack_channel_id" in ids  # still pending


def test_collect_copy_notify_args_reads_only_chosen_destination_fields():
    answers = {
        "copy_notify_destination": "webhook",
        "copy_notify_webhook_url": "https://hooks.example.com",
        "copy_notify_webhook_username": "svc",
        "copy_notify_webhook_password": "",  # blank -> omitted
        "copy_notify_slack_url": "https://leftover.slack",  # belongs to a different dest -> ignored
    }
    args = collect_copy_notify_args(answers)
    assert args == {"url": "https://hooks.example.com", "username": "svc"}


def test_keep_default_does_not_collapse():
    p = _pipeline_with_copy_notify()
    out = apply_configuration(p, TranslationConfiguration())  # default = keep
    names = {t.name for t in out.tasks}
    assert {"Notify Success", "Notify Failure"} <= names  # still present


def test_email_collapse_drops_notifies_and_stamps_copy():
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="email",
        copy_notify_args={"addresses": "a@x.com, b@x.com"},
        copy_notify_events="both",
    )
    out = apply_configuration(p, cfg)
    names = {t.name for t in out.tasks}
    assert "Notify Success" not in names and "Notify Failure" not in names
    copy = next(t for t in out.tasks if t.task_key == "load_curated")
    assert copy.notifications["destination"] == "email"
    assert copy.notifications["args"]["addresses"] == ["a@x.com", "b@x.com"]
    assert set(copy.notifications["events"]) == {"on_success", "on_failure"}
    # downstream task rewired off the dropped notify onto the copy
    after = next(t for t in out.tasks if t.task_key == "after")
    assert any(d.task_key == "load_curated" for d in (after.depends_on or []))


def test_webhook_collapse_stamps_resolved_args():
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="webhook",
        copy_notify_args={"url": "https://hooks.example.com", "username": "svc"},
        copy_notify_events="both",
    )
    out = apply_configuration(p, cfg)
    copy = next(t for t in out.tasks if t.task_key == "load_curated")
    assert copy.notifications["destination"] == "webhook"
    assert copy.notifications["args"] == {"url": "https://hooks.example.com", "username": "svc"}


def test_events_restriction_to_failure_only():
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="email",
        copy_notify_args={"addresses": "a@x.com"},
        copy_notify_events="on_failure",
    )
    out = apply_configuration(p, cfg)
    copy = next(t for t in out.tasks if t.task_key == "load_curated")
    assert copy.notifications["events"] == ["on_failure"]


def test_resolve_email_notifications():
    keys, setup = resolve_task_notifications(
        {"destination": "email", "args": {"addresses": ["a@x.com"]}, "events": ["on_failure", "on_success"]}
    )
    assert keys == {"email_notifications": {"on_failure": ["a@x.com"], "on_success": ["a@x.com"]}}
    assert setup == []


def test_resolve_webhook_without_workspace_falls_back_to_setup_task(monkeypatch):
    # Force the SDK create path to fail -> graceful fallback to a setup task.
    import orchestra.preparer.notifications as nm

    monkeypatch.setattr(nm, "_ensure_destination", lambda *a, **k: None)
    keys, setup = resolve_task_notifications(
        {
            "destination": "slack",
            "args": {"url": "https://hooks"},
            "destination_name": "orchestra-slack",
            "events": ["on_failure"],
        }
    )
    assert keys == {}
    assert len(setup) == 1 and setup[0].type == "notification_destination"
    assert setup[0].config["url"] == "https://hooks"


def test_build_destination_config_passes_only_supplied_optional_fields():
    """Optional SDK kwargs are omitted when blank so the SDK applies its defaults."""
    import orchestra.preparer.notifications as nm

    class _FakeSlackConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class _FakeConfig:
        def __init__(self, slack=None):
            self.slack = slack

    class _FakeSettings:
        Config = _FakeConfig
        SlackConfig = _FakeSlackConfig

    cfg = nm._build_destination_config(_FakeSettings, "slack", {"url": "https://hooks", "channel_id": ""})
    assert cfg.slack.kwargs == {"url": "https://hooks"}  # blank channel_id dropped


def test_validate_answer_accepts_free_text_copy_notify_options():
    """Regression: free-text copy_notify follow-ups must validate (were rejected
    as 'Unknown option_id', so email recipients never reached the config)."""
    import pytest

    from orchestra.adapter.operations import validate_answer

    # free-text options accept any value
    assert validate_answer("copy_notify_email_recipients", "a@x.com, b@x.com") == "a@x.com, b@x.com"
    assert validate_answer("copy_notify_webhook_url", "https://hooks.example.com") == "https://hooks.example.com"
    assert validate_answer("copy_notify_slack_oauth_token", "xoxb-123") == "xoxb-123"
    assert validate_answer("copy_notify_pagerduty_integration_key", "abc123") == "abc123"
    assert validate_answer("copy_notify_destination_name", "orchestra-oncall") == "orchestra-oncall"
    # enum-backed options still validate against their enum
    assert validate_answer("copy_notify_destination", "email") == "email"
    with pytest.raises(ValueError):
        validate_answer("copy_notify_destination", "carrier_pigeon")
    # genuinely unknown ids are still rejected
    with pytest.raises(ValueError):
        validate_answer("totally_unknown_option", "x")


def test_provision_destination_creates_at_prompt_time(monkeypatch):
    """Non-email destinations are created (SDK) at prompt time and the resolved
    id is stamped onto the spec."""
    import orchestra.preparer.notifications as nm

    monkeypatch.setattr(nm, "_ensure_destination", lambda dest, name, args: "dest-abc-1")
    spec = {"destination": "slack", "destination_name": "orchestra-slack", "args": {"url": "https://h"}}
    new_spec, message = nm.provision_destination(spec)
    assert new_spec["destination_id"] == "dest-abc-1"
    assert "Created" in message or "reused" in message.lower()


def test_provision_destination_email_is_passthrough(monkeypatch):
    """Email needs no destination -- provision is a no-op and never calls the SDK."""
    import orchestra.preparer.notifications as nm

    def _boom(*a, **k):
        raise AssertionError("SDK must not be called for email")

    monkeypatch.setattr(nm, "_ensure_destination", _boom)
    spec = {"destination": "email", "args": {"addresses": ["a@x.com"]}}
    new_spec, message = nm.provision_destination(spec)
    assert new_spec is spec
    assert message == ""


def test_provision_destination_failure_keeps_spec(monkeypatch):
    """When creation fails at prompt time the spec is unchanged (args retained) so
    prepare can retry / emit a setup task, and a warning is surfaced."""
    import orchestra.preparer.notifications as nm

    monkeypatch.setattr(nm, "_ensure_destination", lambda *a, **k: None)
    spec = {"destination": "webhook", "args": {"url": "https://h"}}
    new_spec, message = nm.provision_destination(spec)
    assert "destination_id" not in new_spec
    assert new_spec is spec
    assert message.startswith("WARNING")


def test_provision_notification_destinations_walk(monkeypatch):
    """The adapter modify-phase walk stamps resolved ids onto non-email copy tasks."""
    import orchestra.preparer.notifications as nm

    monkeypatch.setattr(nm, "_ensure_destination", lambda dest, name, args: "dest-xyz-9")
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="slack",
        copy_notify_args={"url": "https://hooks.slack.com/x"},
        copy_notify_events="both",
    )
    stamped = apply_configuration(p, cfg)
    provisioned, messages = provision_notification_destinations(stamped)
    copy = next(t for t in provisioned.tasks if t.task_key == "load_curated")
    assert copy.notifications["destination_id"] == "dest-xyz-9"
    assert len(messages) == 1


def test_provision_walk_skips_email_and_keep(monkeypatch):
    """Email collapse produces no destination; the walk makes no SDK call and stamps no id."""
    import orchestra.preparer.notifications as nm

    def _boom(*a, **k):
        raise AssertionError("SDK must not be called for email")

    monkeypatch.setattr(nm, "_ensure_destination", _boom)
    p = _pipeline_with_copy_notify()
    cfg = TranslationConfiguration(
        copy_notify_destination="email", copy_notify_args={"addresses": "a@x.com"}, copy_notify_events="both"
    )
    provisioned, messages = provision_notification_destinations(apply_configuration(p, cfg))
    copy = next(t for t in provisioned.tasks if t.task_key == "load_curated")
    assert "destination_id" not in copy.notifications
    assert messages == []


def test_resolve_uses_pre_resolved_id_without_sdk(monkeypatch):
    """At prepare time a spec carrying a prompt-time destination_id wires directly,
    with no further SDK call."""
    import orchestra.preparer.notifications as nm

    def _boom(*a, **k):
        raise AssertionError("prepare must reuse the prompt-time id, not call the SDK")

    monkeypatch.setattr(nm, "_ensure_destination", _boom)
    keys, setup = resolve_task_notifications(
        {"destination": "slack", "destination_id": "dest-pre-7", "events": ["on_failure"]}
    )
    assert keys == {"webhook_notifications": {"on_failure": [{"id": "dest-pre-7"}]}}
    assert setup == []
