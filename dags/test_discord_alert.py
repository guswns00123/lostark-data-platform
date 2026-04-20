"""
Discord 알림 연동 테스트 DAG.
- success_task: 정상 완료 (알림 X)
- fail_task:    의도적 예외 발생 → Discord 채널에 실패 알림 전송
"""
from datetime import datetime
from airflow.decorators import dag, task

from alerts import discord_failure_callback


@dag(
    dag_id="test_discord_alert",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "alert"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def test_discord_alert():

    @task
    def success_task():
        print("✅ 정상 완료 - 이 task는 알림 안 옴")

    @task
    def fail_task():
        raise Exception("🔥 Discord 알림 테스트용 의도적 실패")

    success_task() >> fail_task()


test_discord_alert()
