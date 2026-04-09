"""
config.py 유닛 테스트.

실제 DB/API 연결 없이 환경변수 로딩 로직만 검증합니다.
"""

import os
import pytest


def test_get_db_conn_params_returns_required_keys(monkeypatch):
    """DB 연결 파라미터가 필수 키를 모두 포함하는지 확인."""
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "testdb")

    from game_chatbot_data.config import get_db_conn_params
    params = get_db_conn_params()

    assert "user" in params
    assert "password" in params
    assert "host" in params
    assert "port" in params
    assert "dbname" in params


def test_get_db_conn_params_values(monkeypatch):
    """환경변수 값이 올바르게 반영되는지 확인."""
    monkeypatch.setenv("DB_USER", "myuser")
    monkeypatch.setenv("DB_PASSWORD", "mypass")
    monkeypatch.setenv("DB_HOST", "10.0.0.1")
    monkeypatch.setenv("DB_PORT", "5433")
    monkeypatch.setenv("DB_NAME", "lostark")

    # 모듈을 재임포트해야 환경변수 변경이 반영됨
    import importlib
    import game_chatbot_data.config as cfg
    importlib.reload(cfg)

    params = cfg.get_db_conn_params()
    assert params["user"] == "myuser"
    assert params["host"] == "10.0.0.1"
    assert params["port"] == "5433"


def test_headers_contains_user_agent():
    """HEADERS에 User-Agent가 포함되는지 확인."""
    from game_chatbot_data.config import HEADERS
    assert "User-Agent" in HEADERS


def test_job_codes_is_nonempty_list():
    """JOB_CODES가 비어있지 않은 리스트인지 확인."""
    from game_chatbot_data.config import JOB_CODES
    assert isinstance(JOB_CODES, list)
    assert len(JOB_CODES) > 0
