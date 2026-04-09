"""
아크패시브 코어 + 효과 크롤링 DAG.

적재 테이블:
  - lostark.ark_passive_core
  - lostark.ark_grid_core
  - lostark.ark_passive
"""

import sys
sys.path.insert(0, "/home/airflow/lostark-data-platform")

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from game_chatbot_data.config import JOB_CODES
from game_chatbot_data.crawlers.ark_passive import run as run_ark_passive

ENV = Variable.get("environment", default_var="prod")
TARGET_JOB_CODES = [102] if ENV == "dev" else JOB_CODES


def crawl_ark_passive_task():
    run_ark_passive(job_codes=TARGET_JOB_CODES)


with DAG(
    dag_id="crawl_ark_passive",
    description="아크패시브 코어 및 효과 크롤링 및 DB 적재",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["lostark", "crawl", "meta"],
) as dag:
    PythonOperator(
        task_id="crawl_and_load_ark_passive",
        python_callable=crawl_ark_passive_task,
    )
