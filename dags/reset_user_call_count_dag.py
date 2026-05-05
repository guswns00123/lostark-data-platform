from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from alerts import discord_failure_callback

CONN_ID = "postgres_lostark"


@dag(
    dag_id="reset_user_call_count",
    schedule="0 0 * * *",  # 매일 00:00 (KST 기준이면 Airflow timezone 확인 필요)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # 동일한 DAG가 동시에 2개 이상 겹쳐서 도는 것을 방지
    max_active_tasks=3,  # 이 DAG 안에서 동시에 실행되는 Task 개수를 최대 3개로 제한
    tags=["lostark", "user", "maintenance"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def reset_user_call_count_dag():

    @task()
    def reset_remaining_call_count():
        query = """
            UPDATE public.user_info_tb
            SET remaining_call_count = 0;
        """

        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = pg_hook.get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    affected = cur.rowcount
                conn.commit()
        finally:
            conn.close()

        print(f"✅ user_info_tb {affected}건 remaining_call_count 초기화 완료")

    reset_remaining_call_count()


reset_user_call_count_dag()
