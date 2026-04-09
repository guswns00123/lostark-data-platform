"""
환경 변수 기반 설정 관리 모듈.
.env 파일에서 DB 접속 정보와 API Key를 로드합니다.
"""

import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

# ── PostgreSQL ──
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_SCHEMA = "lostark"

# ── OpenAI ──
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ── 크롤링 공통 ──
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# 전체 직업 코드 목록
JOB_CODES = [
    102, 103, 104, 105, 112, 113,
    202, 203, 204, 205,
    302, 303, 304, 305, 312, 313,
    402, 403, 404, 405,
    502, 503, 504, 505, 512,
    602, 603, 604,
    702,
]


def get_db_url() -> str:
    """SQLAlchemy용 PostgreSQL 연결 URL을 반환합니다."""
    safe_pw = urllib.parse.quote_plus(DB_PASSWORD)
    return f"postgresql+psycopg2://{DB_USER}:{safe_pw}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_db_conn_params() -> dict:
    """psycopg2.connect()에 전달할 파라미터 딕셔너리를 반환합니다."""
    return {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT,
    }
