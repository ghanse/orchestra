"""Resolve collapsed copy_and_notify notification specs into DAB task notifications.

The adapter stamps a notification spec onto the Copy task when the user opts a
``copy_and_notify`` motif into a Databricks notification destination.  This module
turns that spec into the task-level ``email_notifications`` / ``webhook_notifications``
the bundler emits.  For email it uses raw addresses; for Slack / Microsoft Teams /
PagerDuty / Generic Webhook it creates (or reuses) a Databricks notification
destination via the SDK at prepare time and references its id.
"""

from __future__ import annotations

import logging
from typing import Any

from orchestra.models.dab import SetupTask

logger = logging.getLogger(__name__)

_WEBHOOK_DESTINATIONS = frozenset({"slack", "teams", "pagerduty", "webhook"})


def resolve_task_notifications(spec: dict[str, Any]) -> tuple[dict[str, Any], list[SetupTask]]:
    """Return ``(task_notification_keys, setup_tasks)`` for a notification spec.

    ``task_notification_keys`` is merged into the DAB task dict (it carries an
    ``email_notifications`` or ``webhook_notifications`` entry).  ``setup_tasks``
    is non-empty only when a webhook-style destination could not be created
    (e.g. no workspace auth at prepare time), in which case a documentation
    SetupTask is emitted instead and the task ships without notifications.
    """
    destination = spec.get("destination", "")
    events: list[str] = spec.get("events") or ["on_failure"]

    if destination == "email":
        recipients = [r for r in (spec.get("email_recipients") or []) if r]
        if not recipients:
            logger.warning("copy_and_notify email destination has no recipients; skipping notification wiring.")
            return {}, []
        return {"email_notifications": {event: list(recipients) for event in events}}, []

    if destination not in _WEBHOOK_DESTINATIONS:
        return {}, []

    display_name = spec.get("destination_name") or f"orchestra-{destination}"
    destination_id = _ensure_destination(destination, display_name, spec)
    if destination_id is None:
        setup = SetupTask(
            type="notification_destination",
            config={
                "destination": destination,
                "display_name": display_name,
                "url": spec.get("webhook_url", ""),
                "pagerduty_integration_key": spec.get("pagerduty_integration_key", ""),
                "note": (
                    "Could not create this notification destination during prepare. Create it "
                    "(Settings > Notifications, or w.notification_destinations.create), then add "
                    '{"id": "<destination-id>"} to the task\'s webhook_notifications.'
                ),
            },
        )
        return {}, [setup]

    return {"webhook_notifications": {event: [{"id": destination_id}] for event in events}}, []


def _ensure_destination(destination: str, display_name: str, spec: dict[str, Any]) -> str | None:
    """Create (or reuse) a notification destination via the SDK; return its id or None."""
    try:
        from databricks.sdk.service import settings as sdk_settings

        from orchestra.preparer.workspace_downloader import _get_workspace_client

        client = _get_workspace_client()
        # Reuse an existing destination with the same display name so prepare is idempotent.
        for existing in client.notification_destinations.list():
            if getattr(existing, "display_name", None) == display_name and getattr(existing, "id", None):
                logger.info("Reusing existing notification destination '%s' (%s).", display_name, existing.id)
                return existing.id

        url = spec.get("webhook_url", "")
        if destination == "slack":
            config = sdk_settings.Config(slack=sdk_settings.SlackConfig(url=url))
        elif destination == "teams":
            config = sdk_settings.Config(microsoft_teams=sdk_settings.MicrosoftTeamsConfig(url=url))
        elif destination == "webhook":
            config = sdk_settings.Config(generic_webhook=sdk_settings.GenericWebhookConfig(url=url))
        elif destination == "pagerduty":
            config = sdk_settings.Config(
                pagerduty=sdk_settings.PagerdutyConfig(integration_key=spec.get("pagerduty_integration_key", ""))
            )
        else:
            return None

        created = client.notification_destinations.create(display_name=display_name, config=config)
        logger.info("Created notification destination '%s' (%s).", display_name, getattr(created, "id", None))
        return getattr(created, "id", None)
    except Exception as exc:  # noqa: BLE001 - degrade gracefully to a setup task
        logger.warning("Notification destination create failed for '%s' (%s): %s", display_name, destination, exc)
        return None
