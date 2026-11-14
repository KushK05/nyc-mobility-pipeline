"""Failure alerting for the pipeline: a structured log line plus optional Slack post."""

import logging
import os

import requests

log = logging.getLogger(__name__)


def _describe(context: dict) -> tuple[str, str, str, str]:
    ti = context.get("ti") or context.get("task_instance")
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = getattr(dag, "dag_id", None) or getattr(ti, "dag_id", None) or "unknown"
    task_id = getattr(ti, "task_id", None) or "unknown"
    run_id = context.get("run_id") or getattr(dag_run, "run_id", None) or "unknown"
    log_url = getattr(ti, "log_url", None) or "n/a"
    return dag_id, task_id, run_id, log_url


def on_failure_callback(context: dict) -> None:
    """Log a greppable failure line and post it to Slack when SLACK_WEBHOOK_URL is set.

    Never raises: an alerting problem must not mask the original task failure.
    """
    dag_id, task_id, run_id, log_url = _describe(context)
    message = f"PIPELINE FAILURE dag={dag_id} task={task_id} run={run_id} url={log_url}"
    if context.get("reason"):
        message += f" reason={context['reason']}"
    log.error(message)

    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    try:
        response = requests.post(webhook, json={"text": message}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Slack alert failed: %s", exc)
