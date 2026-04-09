"""
벡터 임베딩 적재 DAG.

Few-shot 예제 및 스키마 메타데이터 임베딩을 순서대로 적재합니다.
크롤링 DAG들이 모두 완료된 후 수동 실행을 권장합니다.

적재 테이블:
  - lostark.few_shot_examples_2
  - lostark.schema_comments_tb
"""

import sys
sys.path.insert(0, "/home/airflow/lostark-data-platform")

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from game_chatbot_data.embeddings.few_shot import run as run_few_shot
from game_chatbot_data.embeddings.schema_embed import run as run_schema_embed

with DAG(
    dag_id="embed_vectors",
    description="Few-shot 및 스키마 메타데이터 벡터 임베딩 적재",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["lostark", "embedding", "pgvector"],
) as dag:
    t1 = PythonOperator(
        task_id="embed_few_shot",
        python_callable=run_few_shot,
    )

    t2 = PythonOperator(
        task_id="embed_schema",
        python_callable=run_schema_embed,
    )

    t1 >> t2  # few_shot 먼저, 그 다음 schema
