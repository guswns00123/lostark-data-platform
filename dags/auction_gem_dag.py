import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor

from extractors import fetch_auction_data
from alerts import discord_failure_callback

CONN_ID = "postgres_lostark"


def parse_auction_options(options_list):
    """API의 Options 리스트를 DB용 JSON 딕셔너리로 변환"""
    if not options_list:
        return "{}"

    parsed = {}
    for opt in options_list:
        opt_name = opt.get("OptionName")
        if not opt_name:
            continue
        if opt.get("IsPenalty"):
            opt_name = f"[감소] {opt_name}"
        parsed[opt_name] = opt.get("Value")

    return json.dumps(parsed, ensure_ascii=False)


@dag(
    dag_id="lostark_auction_gem_collect",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lostark", "auction", "gem"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def auction_gem_collect_dag():

    @task()
    def extract_auction_task(payload: dict, item_label: str, **context):
        api_key = Variable.get("LOSTARK_API_KEY_2")
        items = fetch_auction_data(payload, api_key)

        if not items:
            return None

        exec_date = context['logical_date'].strftime("%Y%m%d_%H%M")
        file_path = f"/tmp/auction_gem_{item_label}_{exec_date}.json"

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False)

        return file_path

    @task()
    def load_auction_task(file_path: str, item_type: str, **context):
        if not file_path or not os.path.exists(file_path):
            print(f"❌ [{item_type}] 처리할 데이터가 없습니다!")
            return

        collected_at = context['logical_date']

        with open(file_path, 'r', encoding='utf-8') as f:
            items = json.load(f)

        params = []
        for item in items:
            auc_info = item.get("AuctionInfo", {})
            options_json = parse_auction_options(item.get("Options", []))

            params.append((
                item_type,
                item.get("Name"),
                item.get("Grade"),
                item.get("Tier"),
                item.get("GradeQuality"),
                auc_info.get("BuyPrice"),
                auc_info.get("BidPrice"),
                auc_info.get("EndDate"),
                options_json,
                collected_at
            ))

        query = """
            INSERT INTO lostark.auction_items_tb (
                item_type, name, grade, tier, quality,
                buy_price, bid_price, end_date, options, collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, params)
            conn.commit()

        print(f"✅ [{item_type}] {len(params)}건 매물 적재 완료")

    prev_group = EmptyOperator(task_id="start_auction_gem_collection")

    # 💡 보석 수집 payload 목록 — 8/9/10레벨
    # fetch_auction_data가 PageNo를 내부에서 1부터 끝까지 자동 순회하므로
    # 여기서는 PageNo=1만 세팅해두면 됨
    target_payloads = [
        {
            "label": "보석_8레벨",
            "type": "보석",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 210000,
                "ItemTier": 4,
                "ItemName": "8레벨",
                "PageNo": 1,
            }
        },
        {
            "label": "보석_9레벨",
            "type": "보석",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 210000,
                "ItemTier": 4,
                "ItemName": "9레벨",
                "PageNo": 1,
            }
        },
        {
            "label": "보석_10레벨",
            "type": "보석",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 210000,
                "ItemTier": 4,
                "ItemName": "10레벨",
                "PageNo": 1,
            }
        },
    ]

    for target in target_payloads:
        wait_20s = TimeDeltaSensor(
            task_id=f"wait_20s_{target['label']}",
            delta=timedelta(seconds=20)
        )
        with TaskGroup(group_id=f"process_auction_{target['label']}") as item_group:

            extracted_file = extract_auction_task.override(task_id=f"extract_{target['label']}")(
                payload=target["payload"],
                item_label=target["label"]
            )

            load_auction_task.override(task_id=f"load_{target['label']}")(
                file_path=extracted_file,
                item_type=target["type"]
            )
        prev_group >> wait_20s >> item_group

        prev_group = item_group


auction_gem_collect_dag()
