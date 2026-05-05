import json
import os
import time
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# 💡 수정됨: 'plugins.' 생략
from extractors import fetch_market_data
from alerts import discord_failure_callback

CONN_ID = "postgres_lostark"


@dag(
    dag_id="lostark_market_collect_1day",
    schedule="0 0 * * *",  # 👈 또는 schedule="@daily" 사용 가능
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # 동일한 DAG가 동시에 2개 이상 겹쳐서 도는 것을 방지
    max_active_tasks=3,  # 이 DAG 안에서 동시에 실행되는 Task 개수를 최대 3개로 제한
    tags=["lostark", "market", "utils"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def lostark_market_collect_1day():

    @task()
    def extract_market_task(payload: dict, item_label: str, **context):
        # 1. 데이터 추출 (utils 함수 사용)
        api_key = Variable.get("LOSTARK_API_KEY_2")
        items = fetch_market_data(payload, api_key)

        if not items:
            return None

        # 2. 파일 저장 (아이템 라벨을 파일명에 포함하여 겹치지 않게 방지)
        exec_date = context["logical_date"].strftime("%Y%m%d_%H%M")
        file_path = f"/tmp/market_items_{item_label}_{exec_date}.json"

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)

        return file_path

    @task()
    def load_market_task(file_path: str, item_type: str, **context):
        if not file_path or not os.path.exists(file_path):
            print("❌ 처리할 데이터가 없습니다.")
            return

        collected_at = context["logical_date"]

        with open(file_path, "r", encoding="utf-8") as f:
            items = json.load(f)

        params = [
            (
                item.get("Id"),
                item.get("Name"),
                item.get("Grade"),
                item.get("Icon"),
                item.get("BundleCount"),
                (
                    item.get("TradeRemainCount")
                    if item.get("TradeRemainCount") is not None
                    else -1
                ),
                item.get("CurrentMinPrice"),
                item.get("YDayAvgPrice"),
                item.get("RecentPrice"),
                collected_at,
                item_type,
            )
            for item in items
        ]

        query = """
            INSERT INTO lostark.market_items_tb (
                item_id, name, grade, icon, bundle_count, 
                trade_remain_count, current_min_price, yday_avg_price, 
                recent_price, collected_at, item_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = pg_hook.get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    execute_batch(cur, query, params)
                conn.commit()
        finally:
            conn.close()

        print(
            f"✅ {item_type} - {len(params)}건의 시세 이력 적재 완료 (시점: {collected_at})"
        )

    target_items = [
        {
            "label": "leap",
            "type": "아바타",
            "payload": {
                "Sort": "GRADE",
                "CategoryCode": 20000,
                "ItemName": "도약",
                "PageNo": 1,
                "SortCondition": "ASC",
            },
        },
        {
            "label": "eternity",
            "type": "아바타",
            "payload": {
                "Sort": "GRADE",
                "CategoryCode": 20000,
                "ItemName": "영원",
                "PageNo": 1,
                "SortCondition": "ASC",
            },
        },
        {
            "label": "chaos_gem",
            "type": "강화재료",
            "payload": {
                "Sort": "GRADE",
                "CategoryCode": 50000,
                "ItemName": "혼돈의 젬",
                "PageNo": 1,
                "SortCondition": "ASC",
            },
        },
        {
            "label": "order_gem",
            "type": "강화재료",
            "payload": {
                "Sort": "GRADE",
                "CategoryCode": 50000,
                "ItemName": "질서의 젬",
                "PageNo": 1,
                "SortCondition": "ASC",
            },
        },
    ]

    # 💡 루프를 돌며 TaskGroup 생성
    for item in target_items:
        # group_id는 Airflow UI에 표시될 이름입니다 (공백 없이 작성)
        with TaskGroup(group_id=f"process_market_{item['label']}") as item_group:

            # 태스크 호출 시 item_label을 함께 넘겨 파일명이 겹치지 않게 합니다.
            # 데코레이터 태스크는 호출 시 이름을 명시적으로 지정할 수 있습니다 (task_id 오버라이드).
            extracted_file = extract_market_task.override(
                task_id=f"extract_{item['label']}"
            )(payload=item["payload"], item_label=item["label"])

            load_market_task.override(task_id=f"load_{item['label']}")(
                file_path=extracted_file, item_type=item["type"]
            )


# DAG 인스턴스화
lostark_market_collect_1day()
