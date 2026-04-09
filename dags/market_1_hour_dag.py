import json
import os
import time
from datetime import datetime

# 🚨 수정됨: Airflow 3.0(sdk) -> 2.x 버전용으로 변경
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from airflow.models import Variable

# 💡 수정됨: 'plugins.' 생략
from extractors import fetch_market_data


CONN_ID = "postgres_lostark"

@dag(
    dag_id="lostark_market_collect",
    schedule="0 * * * *",  # 👈 또는 schedule="@hourly" 사용 가능
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lostark", "market", "utils"],
)
def market_collect_dag():

    @task()
    def extract_market_task(payload: dict, item_label: str, **context):
        # 1. 데이터 추출 
        api_key = Variable.get("LOSTARK_API_KEY") 
        items = fetch_market_data(payload, api_key)
        
        if not items:
            return None

        # 2. 파일 저장 (아이템 라벨을 파일명에 포함하여 덮어쓰기 방지)
        exec_date = context['logical_date'].strftime("%Y%m%d_%H%M")
        file_path = f"/tmp/market_items_{item_label}_{exec_date}.json"
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False)
            
        return file_path

    @task()
    def load_market_task(file_path: str, item_type: str, **context):
        if not file_path or not os.path.exists(file_path):
            print("❌ 처리할 데이터가 없습니다.")
            return

        collected_at = context['logical_date'] 

        with open(file_path, 'r', encoding='utf-8') as f:
            items = json.load(f)

        params = [(
            item.get("Id"), 
            item.get("Name"), 
            item.get("Grade"),
            item.get("Icon"), 
            item.get("BundleCount"), 
            item.get("TradeRemainCount") if item.get("TradeRemainCount") is not None else -1,
            item.get("CurrentMinPrice"), 
            item.get("YDayAvgPrice"), 
            item.get("RecentPrice"),
            collected_at, 
            item_type
        ) for item in items]

        query = """
            INSERT INTO lostark.market_items_tb (
                item_id, name, grade, icon, bundle_count, 
                trade_remain_count, current_min_price, yday_avg_price, 
                recent_price, collected_at, item_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, params)
            conn.commit()
        
        print(f"✅ [{item_type}] {len(params)}건 시세 이력 적재 완료 (시점: {collected_at})")

    # 💡 수집할 4개의 페이로드 목록 정의
    target_items = [
        {
            "label": "engraving", 
            "type": "각인서", # 각인서는 타입을 분리하는 것이 나중에 보기 좋습니다.
            "payload": {"Sort": "GRADE", "CategoryCode": 40000, "ItemGrade": "유물", "ItemName": "각인서", "PageNo": 1, "SortCondition": "ASC"}
        },
        {
            "label": "destiny", 
            "type": "강화재료",
            "payload": {"Sort": "GRADE", "CategoryCode": 50000, "ItemName": "운명", "PageNo": 1, "SortCondition": "ASC"}
        },
        {
            "label": "breath", 
            "type": "강화재료",
            "payload": {"Sort": "GRADE", "CategoryCode": 50000, "ItemName": "숨결", "PageNo": 1, "SortCondition": "ASC"}
        },
        {
            "label": "fusion", 
            "type": "강화재료",
            "payload": {"Sort": "GRADE", "CategoryCode": 50000, "ItemName": "융화 재료", "PageNo": 1, "SortCondition": "ASC"}
        }
    ]

    # 💡 루프를 돌며 TaskGroup 생성 (총 4개의 그룹, 8개의 태스크)
    for item in target_items:
        with TaskGroup(group_id=f"process_market_{item['label']}") as item_group:
            
            # extract 태스크 오버라이드 및 실행
            extracted_file = extract_market_task.override(task_id=f"extract_{item['label']}")(
                payload=item["payload"], 
                item_label=item["label"]
            )
            
            # load 태스크 오버라이드 및 실행
            load_market_task.override(task_id=f"load_{item['label']}")(
                file_path=extracted_file, 
                item_type=item["type"]
            )

# DAG 인스턴스화
market_collect_dag()