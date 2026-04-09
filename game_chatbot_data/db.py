"""
DB 연결 및 적재 공통 유틸리티.
"""

import pandas as pd
from sqlalchemy import create_engine, text

from config import get_db_url, DB_SCHEMA


def get_engine():
    """SQLAlchemy 엔진을 생성하여 반환합니다."""
    return create_engine(get_db_url())


def load_dataframe(df: pd.DataFrame, table_name: str, truncate: bool = False):
    """
    DataFrame을 PostgreSQL 테이블에 적재합니다.

    Args:
        df: 적재할 데이터프레임
        table_name: 대상 테이블명
        truncate: True이면 적재 전 기존 데이터 삭제
    """
    if df.empty:
        print(f"  적재할 데이터가 없습니다. ({table_name})")
        return

    engine = get_engine()

    try:
        if truncate:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{table_name};"))
                conn.commit()
                print(f"  기존 테이블({DB_SCHEMA}.{table_name}) 초기화 완료")

        df.to_sql(
            table_name,
            engine,
            schema=DB_SCHEMA,
            if_exists="append",
            index=False,
        )
        print(f"  {len(df)}건 적재 완료 -> {DB_SCHEMA}.{table_name}")

    except Exception as e:
        print(f"  적재 실패 ({table_name}): {e}")
    finally:
        engine.dispose()
