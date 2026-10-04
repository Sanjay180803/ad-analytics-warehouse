"""Alert callbacks for the ad analytics DAG.

Lives in plugins/ because Airflow puts that folder on sys.path for every component
(scheduler, triggerer, workers), which Deadline Alert callbacks require.

If SLACK_WEBHOOK_URL is set, alerts go to Slack; otherwise they are logged, which
is enough to see them in the task/scheduler logs during local development.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger("ad_analytics.alerting")


def _send(text: str) -> None:
    log.warning(text)
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:  # alerting must never fail the pipeline
        log.error("slack alert failed: %s", exc)


def notify_deadline_missed(context=None, **kwargs) -> None:
    """Deadline Alert callback (Airflow 3's replacement for SLAs)."""
    context = context or {}
    dag_run = context.get("dag_run") or {}
    deadline = context.get("deadline") or {}
    get = (lambda d, k: d.get(k) if isinstance(d, dict) else getattr(d, k, None))
    _send(
        f":hourglass: SLA miss: {get(dag_run, 'dag_id')} run {get(dag_run, 'run_id')} "
        f"(logical date {get(dag_run, 'logical_date')}) was not done by "
        f"{get(deadline, 'deadline_time')}. tier={kwargs.get('tier', 'n/a')}"
    )


def notify_task_failure(context) -> None:
    """on_failure_callback: fires after the last retry is exhausted."""
    ti = context.get("task_instance")
    _send(
        f":red_circle: {ti.dag_id}.{ti.task_id} failed for {context.get('ds')} "
        f"after {ti.try_number} attempt(s). Log: {getattr(ti, 'log_url', '')}"
    )
