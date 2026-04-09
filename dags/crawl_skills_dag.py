"""
스킬 메타 데이터 크롤링 DAG.

실행 조건:
  - schedule=None: 자동 실행 없음, 수동 트리거 전용
  - is_paused_upon_creation=True: 배포 직후 일시정지 상태
  - 대규모 패치 업데이트 시에만 수동 실행

적재 테이블:
  - lostark.lostark_skill_level
  - lostark.lostark_skill_summary
  - lostark.lostark_skill_tripod
"""

import sys
sys.path.insert(0, "/home/airflow/lostark-data-platform")

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from game_chatbot_data.config import JOB_CODES
from game_chatbot_data.crawlers.skills import run as run_skills

# Airflow UI > Admin > Variables > environment 값으로 실행 범위 제어
# dev: 102(워로드) 1개만 테스트 / prod: 전체 직업 코드
ENV = Variable.get("environment", default_var="prod")
TARGET_JOB_CODES = [102] if ENV == "dev" else JOB_CODES


def crawl_skills_task():
    run_skills(job_codes=TARGET_JOB_CODES)


with DAG(
    dag_id="crawl_skills",
    description="로스트아크 스킬 메타 데이터 크롤링 및 DB 적재",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["lostark", "crawl", "meta"],
) as dag:
    PythonOperator(
        task_id="crawl_and_load_skills",
        python_callable=crawl_skills_task,
    )
