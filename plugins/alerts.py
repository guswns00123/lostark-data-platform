import logging
import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def discord_failure_callback(context):
    """Airflow task 실패 시 Discord 채널로 알림 전송"""
    try:
        webhook_url = Variable.get("DISCORD_WEBHOOK_URL")

        ti = context["task_instance"]
        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = ti.run_id
        exec_date = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")
        log_url = ti.log_url
        exception = context.get("exception")

        payload = {
            "username": "Airflow Alert",
            "embeds": [{
                "title": f"❌ Task 실패: {dag_id}.{task_id}",
                "color": 15158332,  # red
                "fields": [
                    {"name": "DAG",       "value": dag_id,   "inline": True},
                    {"name": "Task",      "value": task_id,  "inline": True},
                    {"name": "Run",       "value": run_id,   "inline": False},
                    {"name": "Execution", "value": exec_date, "inline": True},
                    {"name": "Error",     "value": f"```{str(exception)[:500]}```", "inline": False},
                    {"name": "Log",       "value": f"[View Log]({log_url})", "inline": False},
                ],
            }],
        }

        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"✅ Discord 알림 전송 완료: {dag_id}.{task_id}")
    except Exception as e:
        # 알림 실패가 DAG 자체를 fail시키면 안 됨
        logger.error(f"❌ Discord 알림 전송 실패: {e}")
