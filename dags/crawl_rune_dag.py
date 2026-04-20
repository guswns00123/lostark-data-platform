"""
룬 데이터 크롤링 DAG.

적재 테이블:
  - lostark.lostark_rune_tb
"""

import sys
sys.path.insert(0, "/home/airflow/lostark-data-platform")

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from game_chatbot_data.crawlers.rune import run as run_rune
from alerts import discord_failure_callback

with DAG(
    dag_id="crawl_rune",
    description="룬 데이터 크롤링 및 DB 적재",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["lostark", "crawl", "meta"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
) as dag:
    PythonOperator(
        task_id="crawl_and_load_rune",
        python_callable=run_rune,
    )
