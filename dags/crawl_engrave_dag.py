"""
전투 각인 크롤링 DAG (Selenium).

적재 테이블:
  - lostark.engrave
"""

import sys
sys.path.insert(0, "/home/airflow/lostark-data-platform")

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from game_chatbot_data.crawlers.engrave import run as run_engrave
from alerts import discord_failure_callback

with DAG(
    dag_id="crawl_engrave",
    description="전투 각인 Selenium 크롤링 및 DB 적재",
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
        task_id="crawl_and_load_engrave",
        python_callable=run_engrave,
    )
