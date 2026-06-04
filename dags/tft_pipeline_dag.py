"""
TFT 파이프라인 — 상위 티어 → user_game → match_details (6 테이블 적재)

수집 흐름 (티어별 TaskGroup 반복, lostark_auction_collect 스타일)
    ensure_schema
      → start_tft_collection
        → wait_30s_{tier}
          → process_tier_{tier} (TaskGroup):
                extract_top_tier      → load_top_tier
                  → extract_match_ids   → load_user_game
                    → extract_match_details → load_match_details

각 단계는 Riot 응답을 /tmp/*.json 으로 떨어뜨린 뒤(extract) 다음 task 에서 읽어 DB 적재(load).

사전 준비
---------
Airflow Connection
    Conn Id : postgres_lostark  (Postgres, lostark DAG 과 동일 커넥션 공유)

Airflow Variables
    tft_riot_api_key          RGAPI-...      (필수)
    tft_region_platform       kr             (default)
    tft_region_regional       asia           (default)
    tft_queue                 RANKED_TFT     (default)
    tft_max_puuids            300            (default)
    tft_match_ids_per_puuid   10             (default)
    tft_sleep_between_calls   1              (default, 초)
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from psycopg2.extras import execute_batch

# 🚨 Airflow 2.x 데코레이터 + TaskGroup 임포트
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# 💡 plugins 폴더가 루트로 인식되므로 'plugins.' 생략
from alerts import discord_failure_callback

CONN_ID = "postgres_lostark"


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
# Riot API + Variable 헬퍼 (DAG 자체 정의)
# ----------------------------------------------------------------------
def _platform_host(region: str) -> str:
    return f"https://{region}.api.riotgames.com"


def _regional_host(region: str) -> str:
    return f"https://{region}.api.riotgames.com"


def _riot_get(host, path, api_key, params=None, timeout=15):
    url = host + path
    r = requests.get(
        url, headers={"X-Riot-Token": api_key}, params=params, timeout=timeout
    )
    if r.status_code != 200:
        print(f"❌ GET {url} -> {r.status_code} | {r.text[:200]}")
        r.raise_for_status()
    return r.json()


def _cfg():
    """Variable 기반 설정 한 번에 로드 (task 실행 시점에 호출)."""
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
# 응답 파싱 헬퍼 (lostark parse_auction_options 와 같은 역할)
# ----------------------------------------------------------------------
def parse_top_tier_rows(data, region, snapshot_at):
    """top-tier API 응답 → top_tier_info_tb INSERT 행 리스트."""
    rows = []
    for e in data.get("entries", []):
        rows.append(
            (
                snapshot_at,
                e["puuid"],
                region,
                data["queue"],
                data["tier"],
                data["leagueId"],
                data.get("name"),
                e.get("rank"),
                e.get("leaguePoints"),
                e.get("wins"),
                e.get("losses"),
                e.get("veteran"),
                e.get("inactive"),
                e.get("freshBlood"),
                e.get("hotStreak"),
            )
        )
    return rows


def parse_match_detail(data_m, region):
    """match-detail 응답 → (match_info_row, [participant], [trait], [unit])."""
    info = data_m["info"]
    real_mid = data_m["metadata"]["match_id"]
    platform = real_mid.split("_", 1)[0]
    gd_ms = info.get("gameDatetime") or info.get("game_datetime")
    game_dt = datetime.fromtimestamp(gd_ms / 1000, tz=timezone.utc) if gd_ms else None

    match_info_row = (
        real_mid,
        region,
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

    participant_rows, trait_rows, unit_rows = [], [], []
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
            if not t.get("name"):
                continue
            trait_rows.append(
                (
                    mui,
                    t["name"],
                    t.get("num_units"),
                    t.get("style"),
                    t.get("tier_current"),
                    t.get("tier_total"),
                )
            )

        for slot_idx, u in enumerate(p.get("units", [])):
            cid = u.get("character_id") or u.get("characterId")
            if not cid:
                continue
            unit_rows.append(
                (
                    mui,
                    slot_idx,
                    cid,
                    u.get("rarity"),
                    u.get("tier"),
                    u.get("itemNames") or [],
                )
            )

    return match_info_row, participant_rows, trait_rows, unit_rows


# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------
@dag(
    dag_id="tft_collect",
    description="TFT 상위 티어 → user_game → match 6 테이블 적재 (티어별 TaskGroup)",
    schedule=None,  # 수동 트리거. daily 원하면 "0 6 * * *" 등으로 변경
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tft", "riot-api", "taskgroup"],
    max_active_runs=1,
    max_active_tasks=3,
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def tft_collect_dag():

    @task()
    def ensure_schema():
        """6 테이블 DDL (IF NOT EXISTS) 적용."""
        pg = PostgresHook(postgres_conn_id=CONN_ID)
        conn = pg.get_conn()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(DDL_SQL)
        finally:
            conn.close()
        print("✅ DDL 완료 — 6 테이블 준비됨")

    # ---- 1단계 : 상위 티어 entries 수집 -------------------------------
    @task()
    def extract_top_tier(target: dict, **context):
        cfg = _cfg()
        data = _riot_get(
            _platform_host(cfg["region_platform"]),
            target["endpoint"],
            cfg["api_key"],
            params={"queue": cfg["queue"]} if cfg["queue"] else None,
        )
        exec_date = context["logical_date"].strftime("%Y%m%d_%H%M")
        file_path = f"/tmp/tft_top_tier_{target['label']}_{exec_date}.json"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(
            f"✅ [{target['label']}] top_tier entries {len(data.get('entries', []))}개 저장 → {file_path}"
        )
        return file_path

    @task()
    def load_top_tier(file_path: str, target: dict, **context):
        """top_tier_info_tb 적재 + 다음 단계용 puuid 파일 생성."""
        if not file_path or not os.path.exists(file_path):
            print(f"❌ [{target['label']}] top_tier 응답 파일 없음")
            return None

        cfg = _cfg()
        snapshot_at = context["logical_date"]

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        rows = parse_top_tier_rows(data, cfg["region_platform"], snapshot_at)

        if rows:
            query = """
                INSERT INTO tft.top_tier_info_tb (
                    snapshot_at, puuid, region, queue, tier,
                    league_id, league_name, rank,
                    league_points, wins, losses,
                    veteran, inactive, fresh_blood, hot_streak
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_at, puuid) DO NOTHING;
            """
            pg = PostgresHook(postgres_conn_id=CONN_ID)
            conn = pg.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        execute_batch(cur, query, rows)
                    conn.commit()
            finally:
                conn.close()

        # LP 상위 max_puuids 개만 추출
        entries_sorted = sorted(
            data.get("entries", []),
            key=lambda e: e.get("leaguePoints", 0),
            reverse=True,
        )
        puuids = [e["puuid"] for e in entries_sorted][: cfg["max_puuids"]]

        exec_date = context["logical_date"].strftime("%Y%m%d_%H%M")
        puuids_file = f"/tmp/tft_puuids_{target['label']}_{exec_date}.json"
        with open(puuids_file, "w", encoding="utf-8") as f:
            json.dump(puuids, f)
        print(
            f"✅ [{target['label']}] top_tier_info {len(rows)}건 적재, 다음 단계 puuid {len(puuids)}개 → {puuids_file}"
        )
        return puuids_file

    # ---- 2단계 : 각 puuid 의 match_id 수집 ----------------------------
    @task()
    def extract_match_ids(puuids_file: str, target: dict, **context):
        if not puuids_file or not os.path.exists(puuids_file):
            print(f"❌ [{target['label']}] puuids 파일 없음")
            return None

        cfg = _cfg()
        host = _regional_host(cfg["region_regional"])

        with open(puuids_file, "r", encoding="utf-8") as f:
            puuids = json.load(f)

        pairs = []  # [(match_id, puuid), ...]
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
                    pairs.append((mid, p))
            except Exception as exc:
                fail += 1
                print(f"[{i}/{len(puuids)}] {p[:12]} FAIL: {exc}")
            time.sleep(cfg["sleep_s"])

        exec_date = context["logical_date"].strftime("%Y%m%d_%H%M")
        out_file = f"/tmp/tft_match_id_pairs_{target['label']}_{exec_date}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(pairs, f)
        print(
            f"✅ [{target['label']}] match_id 수집 {len(pairs)}개 (fail {fail}) → {out_file}"
        )
        return out_file

    @task()
    def load_user_game(pairs_file: str, target: dict, **context):
        """user_game_info_tb 적재 + 다음 단계용 고유 match_id 파일 생성."""
        if not pairs_file or not os.path.exists(pairs_file):
            print(f"❌ [{target['label']}] match_id pair 파일 없음")
            return None

        cfg = _cfg()

        with open(pairs_file, "r", encoding="utf-8") as f:
            pairs = json.load(f)  # [[match_id, puuid], ...]

        rows = [(p[:12], mid, p, cfg["region_regional"]) for mid, p in pairs]

        if rows:
            query = """
                INSERT INTO tft.user_game_info_tb (puuid_short, game_id, puuid, region)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (puuid_short, game_id) DO NOTHING;
            """
            pg = PostgresHook(postgres_conn_id=CONN_ID)
            conn = pg.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        execute_batch(cur, query, rows)
                    conn.commit()
            finally:
                conn.close()

        unique_mids = sorted({mid for mid, _ in pairs})
        exec_date = context["logical_date"].strftime("%Y%m%d_%H%M")
        out_file = f"/tmp/tft_unique_mids_{target['label']}_{exec_date}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(unique_mids, f)
        print(
            f"✅ [{target['label']}] user_game {len(rows)}건 적재, 고유 match_id {len(unique_mids)}개 → {out_file}"
        )
        return out_file

    # ---- 3단계 : match_id → 상세 응답 + 4 테이블 적재 ----------------
    @task()
    def extract_match_details(mids_file: str, target: dict, **context):
        """match-detail JSON 을 한 줄씩 JSONL 로 떨어뜨림."""
        if not mids_file or not os.path.exists(mids_file):
            print(f"❌ [{target['label']}] unique mids 파일 없음")
            return None

        cfg = _cfg()
        host = _regional_host(cfg["region_regional"])

        with open(mids_file, "r", encoding="utf-8") as f:
            mids = json.load(f)

        exec_date = context["logical_date"].strftime("%Y%m%d_%H%M")
        out_file = f"/tmp/tft_match_details_{target['label']}_{exec_date}.jsonl"

        ok, fail = 0, 0
        with open(out_file, "w", encoding="utf-8") as f:
            for i, mid in enumerate(mids, 1):
                try:
                    data_m = _riot_get(
                        host, f"/tft/match/v1/matches/{mid}", cfg["api_key"]
                    )
                    f.write(json.dumps(data_m, ensure_ascii=False) + "\n")
                    ok += 1
                except Exception as exc:
                    fail += 1
                    print(f"[{i}/{len(mids)}] {mid} FAIL: {exc}")
                time.sleep(cfg["sleep_s"])

        print(
            f"✅ [{target['label']}] match_details 수집 ok {ok} / fail {fail} → {out_file}"
        )
        return out_file

    @task()
    def load_match_details(details_file: str, target: dict, **context):
        """JSONL → 4 테이블 (match_info / participant / trait / unit) 동시 적재.

        한 매치 응답에서 4 곳을 모두 채워야 하므로, child 테이블만 따로 task 로
        쪼개면 동일 JSON 을 한 번 더 staging 으로 내렸다가 다시 읽어야 함.
        같은 task 안에서 한 번에 적재하는 게 가장 효율적.
        """
        if not details_file or not os.path.exists(details_file):
            print(f"❌ [{target['label']}] details 파일 없음")
            return

        cfg = _cfg()
        match_info_rows, participant_rows, trait_rows, unit_rows = [], [], [], []

        with open(details_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data_m = json.loads(line)
                mi, pi, tr, un = parse_match_detail(data_m, cfg["region_regional"])
                match_info_rows.append(mi)
                participant_rows.extend(pi)
                trait_rows.extend(tr)
                unit_rows.extend(un)

        pg = PostgresHook(postgres_conn_id=CONN_ID)
        conn = pg.get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    if match_info_rows:
                        execute_batch(
                            cur,
                            """
                            INSERT INTO tft.match_info_tb (
                                match_id, region, platform, queue_id, tft_game_type,
                                tft_set_number, tft_set_core_name, game_version,
                                game_datetime, game_length, map_id, end_of_game_result
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (match_id) DO NOTHING;
                            """,
                            match_info_rows,
                        )
                    if participant_rows:
                        execute_batch(
                            cur,
                            """
                            INSERT INTO tft.match_participant_info_tb (
                                match_id, puuid, puuid_short,
                                placement, level, last_round, gold_left,
                                time_eliminated, total_damage_to_players,
                                players_eliminated, win,
                                companion_species, companion_skin_id,
                                companion_item_id, companion_content_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (match_id, puuid) DO NOTHING;
                            """,
                            participant_rows,
                        )
                    if trait_rows:
                        execute_batch(
                            cur,
                            """
                            INSERT INTO tft.match_participant_trait_tb (
                                match_user_id, trait_name, num_units, style, tier_current, tier_total
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (match_user_id, trait_name) DO NOTHING;
                            """,
                            trait_rows,
                        )
                    if unit_rows:
                        execute_batch(
                            cur,
                            """
                            INSERT INTO tft.match_participant_unit_tb (
                                match_user_id, slot_idx, unit_name, rarity, tier, item_names
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (match_user_id, slot_idx) DO NOTHING;
                            """,
                            unit_rows,
                        )
                conn.commit()
        finally:
            conn.close()

        print(
            f"✅ [{target['label']}] 적재 완료 — "
            f"match {len(match_info_rows)} / participant {len(participant_rows)} / "
            f"trait {len(trait_rows)} / unit {len(unit_rows)}"
        )

    # ------------------------------------------------------------------
    # 수집 대상 티어 목록 (lostark target_payloads 와 같은 패턴)
    # — grandmaster / master 도 받고 싶으면 주석만 풀면 됩니다.
    # ------------------------------------------------------------------
    target_tiers = [
        {"label": "challenger", "endpoint": "/tft/league/v1/challenger"},
        # {"label": "grandmaster", "endpoint": "/tft/league/v1/grandmaster"},
        # {"label": "master",      "endpoint": "/tft/league/v1/master"},
    ]

    schema_ok = ensure_schema()
    prev_group = EmptyOperator(task_id="start_tft_collection")
    schema_ok >> prev_group

    # TaskGroup 생성 루프
    for target in target_tiers:
        label = target["label"]
        wait_30s = TimeDeltaSensor(
            task_id=f"wait_30s_{label}", delta=timedelta(seconds=30)
        )
        with TaskGroup(group_id=f"process_tier_{label}") as tier_group:

            top_file = extract_top_tier.override(task_id=f"extract_top_tier_{label}")(
                target=target
            )

            puuids_file = load_top_tier.override(task_id=f"load_top_tier_{label}")(
                file_path=top_file, target=target
            )

            pairs_file = extract_match_ids.override(
                task_id=f"extract_match_ids_{label}"
            )(puuids_file=puuids_file, target=target)

            mids_file = load_user_game.override(task_id=f"load_user_game_{label}")(
                pairs_file=pairs_file, target=target
            )

            details_file = extract_match_details.override(
                task_id=f"extract_match_details_{label}"
            )(mids_file=mids_file, target=target)

            load_match_details.override(task_id=f"load_match_details_{label}")(
                details_file=details_file, target=target
            )

        prev_group >> wait_30s >> tier_group
        prev_group = tier_group


tft_collect_dag()
