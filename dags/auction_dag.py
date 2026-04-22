import json
import os
from datetime import datetime, timedelta

# 🚨 수정됨: Airflow 2.x 버전의 데코레이터 및 TaskGroup 임포트 경로
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup



from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# 💡 수정됨: plugins 폴더가 루트 경로로 인식되므로 'plugins.' 생략
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
        # 페널티(감소 효과)는 이름에 표시
        if opt.get("IsPenalty"):
            opt_name = f"[감소] {opt_name}"
        parsed[opt_name] = opt.get("Value")
        
    return json.dumps(parsed, ensure_ascii=False)


@dag(
    dag_id="lostark_auction_collect",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lostark", "auction", "taskgroup"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def auction_collect_dag():

    

    @task()
    def extract_auction_task(payload: dict, item_label: str, **context):
        api_key = Variable.get("LOSTARK_API_KEY") 
        items = fetch_auction_data(payload, api_key)
        
        if not items:
            return None

        exec_date = context['logical_date'].strftime("%Y%m%d_%H%M")
        file_path = f"/tmp/auction_items_{item_label}_{exec_date}.json"
        
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
                options_json,      # 💡 정제된 JSONB 옵션
                collected_at
            ))

        # DDL에서 만든 테이블에 INSERT
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

    prev_group = EmptyOperator(task_id="start_auction_collection")
    # 💡 수집할 경매장 페이로드 목록 (계속 추가 가능!)
    target_payloads = [
        {
            "label": "반지_상하_1", 
            "type": "반지",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 200030,
                "ItemTier": 4,
                "ItemGrade": "고대",
                "ItemName": "반지",
                "PageNo": 1,
                "ItemGradeQuality": None,
                "EtcOptions": [
                    {"FirstOption": 7, "SecondOption": 49, "MinValue": 155, "MaxValue": 155}, 
                    {"FirstOption": 7, "SecondOption": 50, "MinValue": 48, "MaxValue": 400}, 
                    {"FirstOption": 1, "SecondOption": 11, "MinValue": 11000, "MaxValue": 18000}
                ]
            }
        },
        {
            "label": "반지_상하_2", 
            "type": "반지",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 200030,
                "ItemTier": 4,
                "ItemGrade": "고대",
                "ItemName": "반지",
                "PageNo": 1,
                "ItemGradeQuality": None,
                "EtcOptions": [
                    {"FirstOption": 7, "SecondOption": 49, "MinValue": 19, "MaxValue": 155}, 
                    {"FirstOption": 7, "SecondOption": 50, "MinValue": 400, "MaxValue": 400}, 
                    {"FirstOption": 1, "SecondOption": 11, "MinValue": 11000, "MaxValue": 18000}
                ]
            }
        },
        {
            "label": "귀걸이_상하_1", 
            "type": "귀걸이",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 200020,
                "ItemTier": 4,
                "ItemGrade": "고대",
                "ItemName": "귀걸이",
                "PageNo": 1,
                "ItemGradeQuality": None,
                "EtcOptions": [
                    {
                        "FirstOption": 7,
                        "SecondOption": 45,
                        "MinValue": 155,
                        "MaxValue": 155,
                    }, 
                    {
                        "FirstOption": 7,
                        "SecondOption": 46,
                        "MinValue": 36,
                        "MaxValue": 300,
                    }, 
                    {
                        "FirstOption": 1,
                        "SecondOption": 11,
                        "MinValue": 11000,
                        "MaxValue": 18000,
                    },
                ]
            }
        },
        {
            "label": "귀걸이_상하_2", 
            "type": "귀걸이",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 200020,
                "ItemTier": 4,
                "ItemGrade": "고대",
                "ItemName": "귀걸이",
                "PageNo": 1,
                "ItemGradeQuality": None,
                "EtcOptions": [
                    {
                        "FirstOption": 7,
                        "SecondOption": 45,
                        "MinValue": 19,
                        "MaxValue": 155,
                    }, 
                    {
                        "FirstOption": 7,
                        "SecondOption": 46,
                        "MinValue": 300,
                        "MaxValue": 300,
                    }, 
                    {
                        "FirstOption": 1,
                        "SecondOption": 11,
                        "MinValue": 11000,
                        "MaxValue": 18000,
                    },
                ]
            }
        },
        {
            "label": "목걸이_상하_1", 
            "type": "목걸이",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 200010,
                "ItemTier": 4,
                "ItemGrade": "고대",
                "ItemName": "목걸이",
                "PageNo": 1,
                "ItemGradeQuality": None,
                "EtcOptions": [
                    {"FirstOption": 7, "SecondOption": 42, "MinValue": 200, "MaxValue": 200}, 
                    {"FirstOption": 7, "SecondOption": 41, "MinValue": 31, "MaxValue": 260}, 
                    {"FirstOption": 1, "SecondOption": 11, "MinValue": 15000, "MaxValue": 18000}
                ]
            }
        },
        {
            "label": "목걸이_상하_2", 
            "type": "목걸이",
            "payload": {
                "Sort": "BUY_PRICE",
                "SortCondition": "ASC",
                "CategoryCode": 200010,
                "ItemTier": 4,
                "ItemGrade": "고대",
                "ItemName": "목걸이",
                "PageNo": 1,
                "ItemGradeQuality": None,
                "EtcOptions": [
                    {"FirstOption": 7, "SecondOption": 42, "MinValue": 24, "MaxValue": 200}, 
                    {"FirstOption": 7, "SecondOption": 41, "MinValue": 260, "MaxValue": 260}, 
                    {"FirstOption": 1, "SecondOption": 11, "MinValue": 15000, "MaxValue": 18000}
                ]
            }
        },
        {
            "label": "치특_팔찌_1", # 둘 다 100 이상 (100+100=200)
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 15, "MinValue": 100, "MaxValue": 120},
                    {"FirstOption": 2, "SecondOption": 16, "MinValue": 100, "MaxValue": 120},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },
        {
            "label": "치특_팔찌_2", # 치명 극상(120), 특화 보완(80~99)
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 15, "MinValue": 120, "MaxValue": 120},
                    {"FirstOption": 2, "SecondOption": 16, "MinValue": 80, "MaxValue": 99},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },
        {
            "label": "치특_팔찌_3", # 특화 극상(120), 치명 보완(80~99)
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 15, "MinValue": 80, "MaxValue": 99},
                    {"FirstOption": 2, "SecondOption": 16, "MinValue": 120, "MaxValue": 120},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },

        # --- 2. 치명(15) + 신속(18) 조합 ---
        {
            "label": "치신_팔찌_1",
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 15, "MinValue": 100, "MaxValue": 120},
                    {"FirstOption": 2, "SecondOption": 18, "MinValue": 100, "MaxValue": 120},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },
        {
            "label": "치신_팔찌_2",
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 15, "MinValue": 120, "MaxValue": 120},
                    {"FirstOption": 2, "SecondOption": 18, "MinValue": 80, "MaxValue": 99},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },
        {
            "label": "치신_팔찌_3",
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 15, "MinValue": 80, "MaxValue": 99},
                    {"FirstOption": 2, "SecondOption": 18, "MinValue": 120, "MaxValue": 120},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },

        # --- 3. 특화(16) + 신속(18) 조합 ---
        {
            "label": "특신_팔찌_1",
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 16, "MinValue": 100, "MaxValue": 120},
                    {"FirstOption": 2, "SecondOption": 18, "MinValue": 100, "MaxValue": 120},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },
        {
            "label": "특신_팔찌_2",
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 16, "MinValue": 120, "MaxValue": 120},
                    {"FirstOption": 2, "SecondOption": 18, "MinValue": 80, "MaxValue": 99},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        },
        {
            "label": "특신_팔찌_3",
            "type": "팔찌",
            "payload": {
                "CategoryCode": 200040, "ItemTier": 4, "ItemGrade": "고대", "ItemName": "팔찌", "PageNo": 1,
                "EtcOptions": [
                    {"FirstOption": 2, "SecondOption": 16, "MinValue": 80, "MaxValue": 99},
                    {"FirstOption": 2, "SecondOption": 18, "MinValue": 120, "MaxValue": 120},
                    {"FirstOption": 4, "SecondOption": 2, "MinValue": 3, "MaxValue": 3}
                ]
            }
        }
        # 다른 부위나 세팅이 필요하면 여기에 딕셔너리만 계속 복붙해서 추가하시면 됩니다.
    ]

    # TaskGroup 생성 루프
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

auction_collect_dag()