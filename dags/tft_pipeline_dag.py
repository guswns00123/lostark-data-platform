"""
TFT 파이프라인 — Challenger → user_game → match_details

원본 노트북: notebooks/tft_pipeline.ipynb 를 Airflow DAG 으로 변환.

Task 구조 (선형):
    ensure_schema
        → fetch_challenger_load_top_tier        (XCom: puuids)
            → fetch_match_ids_load_user_game    (XCom: unique_match_ids)
                → fetch_and_load_match_details  (한 호출당 4 테이블 동시 적재)

사전 준비
---------
Airflow Connection
    Conn Id   : tft_postgres
    Conn Type : Postgres
    Host      : 34.64.79.177
    Schema    : postgres
    Login     : postgres
    Password  : ********
    Port      : 5432

Airflow Variables (Admin → Variables)
    tft_riot_api_key          RGAPI-... (필수)
    tft_region_platform       kr            (default)
    tft_region_regional       asia          (default)
    tft_queue                 RANKED_TFT    (default)
    tft_max_puuids            300           (default)
    tft_match_ids_per_puuid   10            (default)
    tft_sleep_between_calls   1             (default, 초)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import requests
from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_lostark"


# ----------------------------------------------------------------------
# DDL — 6 테이블 IF NOT EXISTS
# ----------------------------------------------------------------------
DDL_SQL = """
CREATE SCHEMA IF NOT EXISTS tft;

CREATE TABLE IF NOT EXISTS tft.top_tier_info_tb (
    snapshot_at     TIMESTAMPTZ NOT NULL,
    puuid           TEXT        NOT NULL,
    region          TEXT        NOT NULL,
    queue           TEXT        NOT NULL,
    tier            TEXT        NOT NULL,
    league_id       TEXT        NOT NULL,
    league_name     TEXT,
    rank            TEXT,
    league_points   INTEGER     NOT NULL,
    wins            INTEGER     NOT NULL,
    losses          INTEGER     NOT NULL,
    veteran         BOOLEAN,
    inactive        BOOLEAN,
    fresh_blood     BOOLEAN,
    hot_streak      BOOLEAN,
    CONSTRAINT challenger_snapshot_pkey PRIMARY KEY (snapshot_at, puuid)
);

CREATE TABLE IF NOT EXISTS tft.user_game_info_tb (
    puuid_short    CHAR(12)    NOT NULL,
    game_id        TEXT        NOT NULL,
    match_user_id  TEXT        GENERATED ALWAYS AS (puuid_short::text || '_' || game_id) STORED NOT NULL,
    puuid          TEXT        NOT NULL,
    region         TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT user_game_pkey PRIMARY KEY (match_user_id),
    CONSTRAINT user_game_puuid_short_game_id_key UNIQUE (puuid_short, game_id)
);

CREATE TABLE IF NOT EXISTS tft.match_info_tb (
    match_id           TEXT        NOT NULL CONSTRAINT match_pkey PRIMARY KEY,
    region             TEXT        NOT NULL,
    platform           TEXT,
    queue_id           INTEGER,
    tft_game_type      TEXT,
    tft_set_number     INTEGER,
    tft_set_core_name  TEXT,
    game_version       TEXT,
    game_datetime      TIMESTAMPTZ,
    game_length        NUMERIC,
    map_id             INTEGER,
    end_of_game_result TEXT,
    inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tft.match_participant_info_tb (
    match_id                TEXT     NOT NULL
        CONSTRAINT match_participant_match_id_fkey
            REFERENCES tft.match_info_tb ON DELETE CASCADE,
    puuid                   TEXT     NOT NULL,
    puuid_short             CHAR(12) NOT NULL,
    match_user_id           TEXT     GENERATED ALWAYS AS (puuid_short::text || '_' || match_id) STORED NOT NULL
        CONSTRAINT match_participant_pkey PRIMARY KEY,
    placement               INTEGER  NOT NULL,
    level                   INTEGER,
    last_round              INTEGER,
    gold_left               INTEGER,
    time_eliminated         NUMERIC,
    total_damage_to_players INTEGER,
    players_eliminated      INTEGER,
    win                     BOOLEAN,
    companion_species       TEXT,
    companion_skin_id       INTEGER,
    companion_item_id       INTEGER,
    companion_content_id    TEXT,
    CONSTRAINT match_participant_match_id_puuid_key UNIQUE (match_id, puuid)
);

CREATE TABLE IF NOT EXISTS tft.match_participant_trait_tb (
    match_user_id   TEXT    NOT NULL
        CONSTRAINT match_participant_trait_match_user_id_fkey
            REFERENCES tft.match_participant_info_tb ON DELETE CASCADE,
    trait_name      TEXT    NOT NULL,
    num_units       INTEGER,
    style           INTEGER,
    tier_current    INTEGER,
    tier_total      INTEGER,
    CONSTRAINT match_participant_trait_pkey PRIMARY KEY (match_user_id, trait_name)
);

CREATE TABLE IF NOT EXISTS tft.match_participant_unit_tb (
    match_user_id   TEXT    NOT NULL
        CONSTRAINT match_participant_unit_match_user_id_fkey
            REFERENCES tft.match_participant_info_tb ON DELETE CASCADE,
    slot_idx        INTEGER NOT NULL,
    unit_name       TEXT    NOT NULL,
    rarity          INTEGER,
    tier            INTEGER,
    item_names      TEXT[],
    CONSTRAINT match_participant_unit_pkey PRIMARY KEY (match_user_id, slot_idx)
);
"""


# ----------------------------------------------------------------------
# Riot API 헬퍼 (helpers.py 의존성 제거 — DAG 안에서 자체 정의)
# ----------------------------------------------------------------------
def _platform_host(region: str) -> str:
    return f"https://{region}.api.riotgames.com"


def _regional_host(region: str) -> str:
    return f"https://{region}.api.riotgames.com"


def _riot_get(
    host: str, path: str, api_key: str, params: dict | None = None, timeout: int = 15
):
    url = host + path
    r = requests.get(
        url, headers={"X-Riot-Token": api_key}, params=params, timeout=timeout
    )
    if r.status_code != 200:
        log.warning("GET %s -> %s | %s", url, r.status_code, r.text[:200])
        r.raise_for_status()
    return r.json()


def _cfg() -> dict:
    """Variable 기반 설정 한 번에 로드."""
    return {
        "api_key": Variable.get("tft_riot_api_key"),
        "region_platform": Variable.get("tft_region_platform", default_var="kr"),
        "region_regional": Variable.get("tft_region_regional", default_var="asia"),
        "queue": Variable.get("tft_queue", default_var="RANKED_TFT"),
        "max_puuids": int(Variable.get("tft_max_puuids", default_var="300")),
        "match_ids_per_puuid": int(
            Variable.get("tft_match_ids_per_puuid", default_var="10")
        ),
        "sleep_s": float(Variable.get("tft_sleep_between_calls", default_var="1")),
    }


# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------
@dag(
    dag_id="tft_pipeline",
    description="TFT Challenger → user_game → match 적재 (6 테이블)",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 수동 트리거. daily 원하면 "@daily"
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tft",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["tft", "riot-api"],
)
def tft_pipeline():

    # ------------------------------------------------------------------
    @task
    def ensure_schema() -> str:
        """6 테이블 DDL (IF NOT EXISTS) 적용."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(DDL_SQL)
        finally:
            conn.close()
        log.info("DDL 완료 — 6 테이블 준비됨")
        return "ok"

    # ------------------------------------------------------------------
    @task
    def fetch_challenger_load_top_tier() -> list[str]:
        """
        Challenger API → top_tier_info_tb INSERT.
        LP 내림차순 상위 N(MAX_PUUIDS) 명의 puuid 를 XCom 반환.
        """
        cfg = _cfg()
        data_chall = _riot_get(
            _platform_host(cfg["region_platform"]),
            "/tft/league/v1/challenger",
            cfg["api_key"],
            params={"queue": cfg["queue"]} if cfg["queue"] else None,
        )

        snapshot_at = datetime.now(timezone.utc)
        entries = data_chall.get("entries", [])
        rows = [
            (
                snapshot_at,
                e["puuid"],
                cfg["region_platform"],
                data_chall["queue"],
                data_chall["tier"],
                data_chall["leagueId"],
                data_chall.get("name"),
                e.get("rank"),
                e.get("leaguePoints"),
                e.get("wins"),
                e.get("losses"),
                e.get("veteran"),
                e.get("inactive"),
                e.get("freshBlood"),
                e.get("hotStreak"),
            )
            for e in entries
        ]

        inserted = 0
        if rows:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO tft.top_tier_info_tb (
                            snapshot_at, puuid, region, queue, tier,
                            league_id, league_name, rank,
                            league_points, wins, losses,
                            veteran, inactive, fresh_blood, hot_streak
                        )
                        VALUES %s
                        ON CONFLICT (snapshot_at, puuid) DO NOTHING
                        """,
                        rows,
                    )
                    inserted = cur.rowcount
            finally:
                conn.close()

        entries_sorted = sorted(
            entries, key=lambda e: e.get("leaguePoints", 0), reverse=True
        )
        puuids = [e["puuid"] for e in entries_sorted][: cfg["max_puuids"]]
        log.info(
            "snapshot_at=%s | entries=%d | inserted=%d | next puuids=%d",
            snapshot_at.isoformat(),
            len(entries),
            inserted,
            len(puuids),
        )
        return puuids

    # ------------------------------------------------------------------
    @task
    def fetch_match_ids_load_user_game(puuids: list[str]) -> list[str]:
        """
        각 puuid → match-id 목록 (MATCH_IDS_PER_PUUID 개) → user_game_info_tb INSERT.
        고유 match_id 정렬 리스트를 XCom 반환.
        """
        cfg = _cfg()
        host = _regional_host(cfg["region_regional"])

        all_user_matches: list[tuple[str, str]] = []  # (match_id, puuid)
        fail = 0
        for i, p in enumerate(puuids, 1):
            try:
                ids = _riot_get(
                    host,
                    f"/tft/match/v1/matches/by-puuid/{p}/ids",
                    cfg["api_key"],
                    params={"start": 0, "count": cfg["match_ids_per_puuid"]},
                )
                for mid in ids:
                    all_user_matches.append((mid, p))
            except Exception as exc:
                log.warning("[%d/%d] %s... FAIL: %s", i, len(puuids), p[:12], exc)
                fail += 1
            time.sleep(cfg["sleep_s"])

        rows = [(p[:12], mid, p, cfg["region_regional"]) for mid, p in all_user_matches]

        inserted = 0
        if rows:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO tft.user_game_info_tb (puuid_short, game_id, puuid, region)
                        VALUES %s
                        ON CONFLICT (puuid_short, game_id) DO NOTHING
                        """,
                        rows,
                    )
                    inserted = cur.rowcount
            finally:
                conn.close()

        unique_match_ids = sorted({mid for mid, _ in all_user_matches})
        log.info(
            "success=%d fail=%d | user_game inserted=%d | unique match_ids=%d",
            len(puuids) - fail,
            fail,
            inserted,
            len(unique_match_ids),
        )
        return unique_match_ids

    # ------------------------------------------------------------------
    @task
    def fetch_and_load_match_details(match_ids: list[str]) -> dict:
        """
        각 match_id → /matches/{id} 호출.
        한 호출의 응답에서 4 테이블 (match_info / participant / trait / unit) 동시 적재.
        Riot detail 한 번 받으면 4 곳에 다 들어가야 하므로 한 task 안에서 처리하는 게
        가장 효율적 — child 테이블만 따로 task 분리하면 같은 데이터를 staging 으로 한 번
        더 내렸다가 다시 읽어야 함.
        """
        cfg = _cfg()
        host = _regional_host(cfg["region_regional"])

        match_info_rows: list[tuple] = []
        participant_rows: list[tuple] = []
        trait_rows: list[tuple] = []
        unit_rows: list[tuple] = []

        ok = 0
        for i, mid in enumerate(match_ids, 1):
            try:
                data_m = _riot_get(host, f"/tft/match/v1/matches/{mid}", cfg["api_key"])
            except Exception as exc:
                log.warning("[%d/%d] %s FAIL: %s", i, len(match_ids), mid, exc)
                time.sleep(cfg["sleep_s"])
                continue

            info = data_m["info"]
            real_mid = data_m["metadata"]["match_id"]
            platform = real_mid.split("_", 1)[0]
            gd_ms = info.get("gameDatetime") or info.get("game_datetime")
            game_dt = (
                datetime.fromtimestamp(gd_ms / 1000, tz=timezone.utc) if gd_ms else None
            )

            match_info_rows.append(
                (
                    real_mid,
                    cfg["region_regional"],
                    platform,
                    info.get("queueId") or info.get("queue_id"),
                    info.get("tft_game_type"),
                    info.get("tft_set_number"),
                    info.get("tft_set_core_name"),
                    info.get("gameVersion") or info.get("game_version"),
                    game_dt,
                    info.get("gameLength") or info.get("game_length"),
                    info.get("mapId") or info.get("map_id"),
                    info.get("endOfGameResult") or info.get("end_of_game_result"),
                )
            )

            for p in info.get("participants", []):
                puuid = p["puuid"]
                puuid_short = puuid[:12]
                mui = f"{puuid_short}_{real_mid}"
                comp = p.get("companion") or {}

                participant_rows.append(
                    (
                        real_mid,
                        puuid,
                        puuid_short,
                        p.get("placement"),
                        p.get("level"),
                        p.get("last_round"),
                        p.get("gold_left"),
                        p.get("time_eliminated"),
                        p.get("total_damage_to_players"),
                        p.get("players_eliminated"),
                        p.get("win"),
                        comp.get("species"),
                        comp.get("skin_ID"),
                        comp.get("item_ID"),
                        comp.get("content_ID"),
                    )
                )

                for t in p.get("traits", []):
                    trait_rows.append(
                        (
                            mui,
                            t.get("name"),
                            t.get("num_units"),
                            t.get("style"),
                            t.get("tier_current"),
                            t.get("tier_total"),
                        )
                    )

                for slot_idx, u in enumerate(p.get("units", [])):
                    unit_rows.append(
                        (
                            mui,
                            slot_idx,
                            u.get("character_id") or u.get("characterId"),
                            u.get("rarity"),
                            u.get("tier"),
                            u.get("itemNames") or [],
                        )
                    )

            ok += 1
            time.sleep(cfg["sleep_s"])

        # NOT NULL 가드
        trait_rows_ok = [r for r in trait_rows if r[1]]
        unit_rows_ok = [r for r in unit_rows if r[2]]

        ins = {"match_info": 0, "participant": 0, "trait": 0, "unit": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                if match_info_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO tft.match_info_tb (
                            match_id, region, platform, queue_id, tft_game_type,
                            tft_set_number, tft_set_core_name, game_version,
                            game_datetime, game_length, map_id, end_of_game_result
                        )
                        VALUES %s
                        ON CONFLICT (match_id) DO NOTHING
                        """,
                        match_info_rows,
                    )
                    ins["match_info"] = cur.rowcount
                if participant_rows:
                    execute_values(
                        cur,
                        """
                        INSERT INTO tft.match_participant_info_tb (
                            match_id, puuid, puuid_short,
                            placement, level, last_round, gold_left,
                            time_eliminated, total_damage_to_players,
                            players_eliminated, win,
                            companion_species, companion_skin_id,
                            companion_item_id, companion_content_id
                        )
                        VALUES %s
                        ON CONFLICT (match_id, puuid) DO NOTHING
                        """,
                        participant_rows,
                    )
                    ins["participant"] = cur.rowcount
                if trait_rows_ok:
                    execute_values(
                        cur,
                        """
                        INSERT INTO tft.match_participant_trait_tb (
                            match_user_id, trait_name, num_units, style, tier_current, tier_total
                        )
                        VALUES %s
                        ON CONFLICT (match_user_id, trait_name) DO NOTHING
                        """,
                        trait_rows_ok,
                    )
                    ins["trait"] = cur.rowcount
                if unit_rows_ok:
                    execute_values(
                        cur,
                        """
                        INSERT INTO tft.match_participant_unit_tb (
                            match_user_id, slot_idx, unit_name, rarity, tier, item_names
                        )
                        VALUES %s
                        ON CONFLICT (match_user_id, slot_idx) DO NOTHING
                        """,
                        unit_rows_ok,
                    )
                    ins["unit"] = cur.rowcount
        finally:
            conn.close()

        result = {
            "match_details_ok": ok,
            "match_details_total": len(match_ids),
            **{f"{k}_inserted": v for k, v in ins.items()},
        }
        log.info("적재 결과: %s", result)
        return result

    # ------------------------------------------------------------------
    # 그래프 와이어링
    schema = ensure_schema()
    puuids = fetch_challenger_load_top_tier()
    match_ids = fetch_match_ids_load_user_game(puuids)
    details = fetch_and_load_match_details(match_ids)

    schema >> puuids  # DDL 먼저, 그 다음 API 호출
    # puuids → match_ids → details 는 인자 전달로 자동 의존성 형성


dag = tft_pipeline()
