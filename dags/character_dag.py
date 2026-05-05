import logging
from datetime import datetime, timedelta
import os
import time
import json

# 🚨 수정됨: Airflow 2.x 버전용 데코레이터 임포트
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# 💡 수정됨: 'plugins.' 생략 (plugins 폴더 자체가 최상위 경로로 인식됨)
from extractors import fetch_armory_data
from parsers import (
    parse_tooltip_content,
    split_core_options,
    split_gem_effect,
    strip_html,
    parse_ark_passive_description,
    parse_rank_level,
    parse_avatar_tooltip,
    extract_card_description,
    parse_equipment_tooltip,
    parse_gem_effects,
    clean_number,
    parse_skill_tooltip,
    to_jsonb,
    parse_additional_effect_to_json,
    parse_basic_effect_to_json,
)
from alerts import discord_failure_callback

logger = logging.getLogger(__name__)

BASE_URL = "https://developer-lostark.game.onstove.com"
CONN_ID = "postgres_lostark"
char_name = "황로드유"


@dag(
    dag_id="lostark_character_extract",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lostark", "api", "extract"],
    max_active_runs=1,  # 동일한 DAG가 동시에 2개 이상 겹쳐서 도는 것을 방지
    max_active_tasks=3,  # 이 DAG 안에서 동시에 실행되는 Task 개수를 최대 3개로 제한
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def lostark_ark_passive_etl_dag():
    @task
    def extract_all_target_characters(**context) -> str:
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        api_key = Variable.get("LOSTARK_API_KEY")

        # Airflow 실행 시간을 파일명에 사용하여 덮어쓰기 방지
        run_id_str = context["logical_date"].strftime("%Y%m%d_%H%M%S")
        file_path = f"/tmp/lostark_data_{run_id_str}.json"

        # 1. DB에서 1640 초과 캐릭터 리스트업
        query = "SELECT distinct character_name FROM lostark.character_info_tb WHERE item_avg_level >= 1700;"
        records = pg_hook.get_records(query)

        if not records:
            logging.info("수집할 캐릭터가 없습니다.")
            return []

        character_list = [row[0] for row in records]
        total_chars = len(character_list)
        logging.info(f"총 {total_chars}명의 캐릭터 API 호출을 시작합니다.")

        extracted_results = []

        # 2. API 호출
        for char_name in character_list:
            try:
                raw_data = fetch_armory_data(char_name, api_key)
                if raw_data:
                    extracted_results.append(
                        {"character_name": char_name, "data": raw_data}
                    )
            except Exception as e:
                logging.error(f"{char_name} 호출 실패: {e}")
                continue
            time.sleep(0.6)  # Rate Limit 방어

        # 3. 🌟 추출된 전체 데이터를 JSON 파일로 로컬에 저장
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(extracted_results, f, ensure_ascii=False)

        logging.info(
            f"데이터 파일 저장 완료: {file_path} (총 {len(extracted_results)}건)"
        )

        # 4. XCom으로는 무거운 데이터 대신 '파일 경로'만 가볍게 리턴!
        return file_path

    data_file_path = extract_all_target_characters()

    with TaskGroup(
        group_id="load_tasks", tooltip="데이터베이스 적재 태스크 그룹"
    ) as load_group:

        @task()
        def load_ark_grid_cores(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")
            if not os.path.exists(file_path):
                logging.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            all_core_tuples = []

            # 2. 파일에서 불러온 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                ag = api_data.get("ArkGrid")
                if not ag:
                    return

                grid_slots = ag.get("Slots") or []

                for slot in grid_slots:
                    core_idx = slot.get("Index")
                    # 1차 파싱 (전체 텍스트)
                    core_opt_raw = parse_tooltip_content(
                        slot.get("Tooltip"), "코어 옵션"
                    )
                    # 2차 파싱 (레벨별 딕셔너리로 분할)
                    opts = split_core_options(core_opt_raw)

                    all_core_tuples.append(
                        (
                            char_name,
                            core_idx,
                            collected_at,
                            slot.get("Name"),
                            slot.get("Grade"),
                            slot.get("Point"),
                            slot.get("Icon"),
                            opts["p1"],
                            opts["o1"],
                            opts["p2"],
                            opts["o2"],
                            opts["p3"],
                            opts["o3"],
                            opts["p4"],
                            opts["o4"],
                            opts["p5"],
                            opts["o5"],
                            opts["p6"],
                            opts["o6"],
                        )
                    )

            if not all_core_tuples:
                return

            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.ark_grid_cores_tb
                            (
                                character_name, slot_index, collected_at, name, grade, point, icon,
                                level_1_point, level_1_option, level_2_point, level_2_option,
                                level_3_point, level_3_option, level_4_point, level_4_option,
                                level_5_point, level_5_option, level_6_point, level_6_option
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_core_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()
            logger.info(
                f"코어 데이터({len(all_core_tuples)}건) 컬럼 분할 및 이력 적재 완료!"
            )

        @task()
        def load_ark_grid_gems(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 전달받은 파일 경로의 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 전체 캐릭터의 젬 튜플을 담을 큰 리스트
            all_gem_tuples = []

            # 2. 파일에서 불러온 다수 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                ag = api_data.get("ArkGrid")
                if not ag:
                    continue

                grid_slots = ag.get("Slots") or []

                # 3. 개별 캐릭터 슬롯 및 젬 파싱 로직
                for slot in grid_slots:
                    core_idx = slot.get("Index")
                    for gem in slot.get("Gems") or []:
                        # 1차 파싱: 툴팁에서 젬 텍스트 덩어리 추출
                        gem_eff_raw = parse_tooltip_content(
                            gem.get("Tooltip"), "젬 효과"
                        )

                        # 2차 파싱: 텍스트 덩어리를 9개의 속성으로 분해
                        opts = split_gem_effect(gem_eff_raw)

                        # 총 17개 매핑하여 큰 리스트에 추가
                        all_gem_tuples.append(
                            (
                                char_name,
                                core_idx,
                                gem.get("Index"),
                                collected_at,
                                gem.get("Grade"),
                                gem.get("IsActive"),
                                gem.get("Icon"),
                                opts["req_will"],
                                opts["will_eff"],
                                opts["pt_type"],
                                opts["pt_val"],
                                opts["e1_name"],
                                opts["e1_lvl"],
                                opts["e1_val"],
                                opts["e2_name"],
                                opts["e2_lvl"],
                                opts["e2_val"],
                            )
                        )

            # 4. 적재할 데이터가 아예 없다면 조기 종료
            if not all_gem_tuples:
                logger.info("적재할 젬 데이터가 없습니다.")
                return

            # 5. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.ark_grid_gems_tb
                            (
                                character_name, core_index, gem_index, collected_at, grade, is_active, icon,
                                required_willpower, willpower_efficiency, point_type, point_value,
                                effect_1_name, effect_1_level, effect_1_value,
                                effect_2_name, effect_2_level, effect_2_value
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_gem_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 젬 데이터 총 {len(all_gem_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_ark_passive_points(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 JSON 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 포인트 데이터를 담을 바구니
            all_point_tuples = []

            # 3. 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                ap = api_data.get("ArkPassive") or {}
                points_data = ap.get("Points") or []

                if not points_data:
                    continue

                # 4. 개별 캐릭터의 포인트 데이터 파싱
                for p in points_data:
                    # "6랭크 25레벨" 텍스트를 숫자형 rank, level로 분할
                    raw_desc = p.get("Description")
                    rank_val, level_val = parse_rank_level(raw_desc)

                    all_point_tuples.append(
                        (
                            char_name,
                            p.get("Name"),
                            collected_at,
                            p.get("Value"),
                            rank_val,
                            level_val,
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_point_tuples:
                logger.info("적재할 패시브 포인트 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (executemany)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        # 💡 DELETE/UPSERT 없이 INSERT 문으로 이력 누적
                        cur.executemany(
                            """
                            INSERT INTO lostark.ark_passive_points_tb
                            (character_name, name, collected_at, value, point_rank, point_level)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                            all_point_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 포인트 데이터 총 {len(all_point_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_ark_passive_effects(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 효과 데이터를 담을 큰 리스트
            all_effect_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                ap = api_data.get("ArkPassive") or {}
                effects_data = ap.get("Effects") or []

                if not effects_data:
                    continue

                # 4. 개별 캐릭터의 패시브 효과 파싱
                for e in effects_data:
                    raw_desc = e.get("Description")
                    # 기존에 작성해두신 파싱 함수 사용
                    tier, effect_name, level = parse_ark_passive_description(raw_desc)

                    all_effect_tuples.append(
                        (
                            char_name,
                            e.get("Name"),
                            collected_at,
                            e.get("Icon"),
                            tier,
                            effect_name,
                            level,
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_effect_tuples:
                logger.info("적재할 패시브 효과 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.ark_passive_effects_tb
                            (character_name, name, collected_at, icon, tier, effect_name, level)
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_effect_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 패시브 효과 데이터 총 {len(all_effect_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_avatars(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 아바타 데이터를 담을 큰 리스트
            all_av_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                av = api_data.get("ArmoryAvatars")

                # 아바타를 장착하지 않은 캐릭터는 건너뜀
                if not av:
                    continue

                # 4. 개별 캐릭터의 아바타 파싱
                for i in av:
                    # 아바타 상세 파싱
                    opts = parse_avatar_tooltip(i.get("Tooltip"))

                    all_av_tuples.append(
                        (
                            char_name,
                            i.get("Name"),
                            collected_at,
                            i.get("Type"),
                            i.get("Icon"),
                            i.get("Grade"),
                            i.get("IsSet"),
                            i.get("IsInner"),
                            opts.get("basic_stat"),  # 예: '힘'
                            opts.get("basic_val"),  # 예: 2.00
                            opts.get("intellect"),  # 예: 5
                            opts.get("courage"),  # 예: 5
                            opts.get("charm"),  # 예: 5
                            opts.get("kindness"),  # 예: 5
                            opts.get("source"),
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_av_tuples:
                logger.info("적재할 아바타 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        # 💡 DELETE 없이 시간대별 이력 적재
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_avatars_tb
                            (
                                character_name, name, collected_at, type, icon, grade, is_set, is_inner,
                                basic_effect_stat, basic_effect_value,
                                tendency_intellect, tendency_courage, tendency_charm, tendency_kindness,
                                source
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_av_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 아바타 데이터 총 {len(all_av_tuples)}건 (다수 캐릭터 분량) 스탯 분할 및 일괄 적재 완료!"
            )

        @task()
        def load_armory_cards(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 카드 장착 데이터를 담을 큰 리스트
            all_card_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                card_data = api_data.get("ArmoryCard")
                if not card_data:
                    continue

                cards = card_data.get("Cards", [])
                if not cards:
                    continue

                # 4. 개별 캐릭터의 카드 데이터 파싱
                for c in cards:
                    desc = extract_card_description(c.get("Tooltip"))

                    all_card_tuples.append(
                        (
                            char_name,
                            c.get("Slot"),
                            collected_at,
                            c.get("Name"),
                            c.get("Icon"),
                            c.get("Grade"),
                            c.get("AwakeCount", 0),
                            c.get("AwakeTotal", 0),
                            desc,
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_card_tuples:
                logger.info("적재할 카드 장착 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        # 💡 DELETE 없이 시간대별 이력 적재
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_card_tb
                            (character_name, slot, collected_at, name, icon, grade, awake_count, awake_total, description)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_card_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 카드 장착 데이터 총 {len(all_card_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_card_effects(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 카드 세트 효과를 담을 큰 리스트
            all_effect_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item_data in extracted_data_list:
                char_name = item_data["character_name"]
                api_data = item_data["data"]

                card_data = api_data.get("ArmoryCard")
                if not card_data:
                    continue

                effects = card_data.get("Effects", [])
                if not effects:
                    continue

                # 4. 개별 캐릭터의 카드 효과(세트 효과) 파싱
                # (Effects 안에 여러 세트가 있고, 각 세트마다 Items로 효과가 나뉘어 있음)
                for effect_group in effects:
                    items = effect_group.get("Items", [])
                    for item in items:
                        all_effect_tuples.append(
                            (
                                char_name,
                                item.get("Name"),
                                collected_at,
                                item.get("Description"),
                            )
                        )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_effect_tuples:
                logger.info("적재할 카드 세트 효과 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_card_effects_tb
                            (character_name, effect_name, collected_at, description)
                            VALUES (%s, %s, %s, %s);
                        """,
                            all_effect_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 카드 세트 효과 데이터 총 {len(all_effect_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_collectibles(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 수집형 포인트 요약 데이터를 담을 큰 리스트
            all_summary_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                co = api_data.get("Collectibles")
                if not co:
                    continue

                # 4. 개별 캐릭터의 수집형 포인트 데이터 파싱
                for i in co:
                    all_summary_tuples.append(
                        (
                            char_name,
                            i.get("Type"),
                            collected_at,
                            i.get("Icon"),
                            i.get("Point"),
                            i.get("MaxPoint"),
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_summary_tuples:
                logger.info("적재할 수집형 포인트 요약 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_collectibles_tb
                            (character_name, type, collected_at, icon, point, max_point)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                            all_summary_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 수집형 포인트 요약 데이터 총 {len(all_summary_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_collectible_details(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 상세 데이터를 담을 큰 리스트
            all_detail_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                co = api_data.get("Collectibles")
                if not co:
                    continue

                # 4. 개별 캐릭터의 수집 카테고리(Type) 순회
                for i in co:
                    c_type = i.get("Type")
                    c_points = i.get("CollectiblePoints") or []

                    # 5. 카테고리 내의 상세 포인트 항목 순회
                    for cp in c_points:
                        all_detail_tuples.append(
                            (
                                char_name,
                                c_type,
                                cp.get("PointName"),
                                collected_at,
                                cp.get("Point"),
                                cp.get("MaxPoint"),
                            )
                        )

            # 6. 적재할 데이터가 없으면 조기 종료
            if not all_detail_tuples:
                logger.info("적재할 수집형 포인트 상세 데이터가 없습니다.")
                return

            # 7. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_collectible_details_tb
                            (character_name, type, point_name, collected_at, point, max_point)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                            all_detail_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 수집형 포인트 상세 데이터 총 {len(all_detail_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_engravings(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 각인 데이터를 담을 큰 리스트
            all_engraving_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item in extracted_data_list:
                char_name = item["character_name"]
                api_data = item["data"]

                en = api_data.get("ArmoryEngraving")
                if not en:
                    continue

                ark_passive_effects = en.get("ArkPassiveEffects", [])
                if not ark_passive_effects:
                    continue

                # 4. 개별 캐릭터의 각인 효과 파싱
                for effect in ark_passive_effects:
                    # 툴팁(설명)에서 HTML 태그 제거 (기존 전처리 로직 유지)
                    clean_desc = strip_html(effect.get("Description"))

                    all_engraving_tuples.append(
                        (
                            char_name,
                            effect.get("Name"),
                            collected_at,
                            effect.get("Grade"),
                            effect.get("Level"),
                            effect.get(
                                "AbilityStoneLevel"
                            ),  # 값이 없으면 자동으로 None(NULL) 처리됨
                            clean_desc,
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_engraving_tuples:
                logger.info("적재할 각인 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        # 💡 DELETE 없이 시간대별로 이력 계속 적재 (INSERT)
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_engravings_tb
                            (character_name, name, collected_at, grade, level, ability_stone_level, description)
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_engraving_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 각인 데이터 총 {len(all_engraving_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_equipment(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 장비 데이터를 담을 큰 리스트
            all_eq_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item_data in extracted_data_list:
                char_name = item_data["character_name"]
                api_data = item_data["data"]

                eq = api_data.get("ArmoryEquipment")
                if not eq:
                    continue

                # 4. 개별 캐릭터의 장비 파싱
                # 💡 기존 로직대로 enumerate를 사용하여 slot_index(0, 1, 2...)를 함께 뽑아냅니다.
                for idx, i in enumerate(eq):
                    item_name = i.get("Name")
                    item_type = i.get("Type")

                    # 파싱 헬퍼 함수 호출 (미리 정의해두신 함수 사용)
                    (
                        enh_lvl,
                        qual,
                        tier,
                        basic_eff_raw,  # 👈 텍스트 원본
                        add_eff_raw,  # 👈 텍스트 원본
                        ark_eff_raw,
                        adv_reinf,
                    ) = parse_equipment_tooltip(i.get("Tooltip"), item_name)

                    basic_eff_json = parse_basic_effect_to_json(basic_eff_raw)
                    add_eff_json = parse_additional_effect_to_json(
                        item_type, add_eff_raw
                    )

                    all_eq_tuples.append(
                        (
                            char_name,  # 1
                            idx,  # 2
                            item_type,  # 3
                            item_name,  # 4
                            collected_at,  # 5
                            i.get("Icon"),  # 6
                            i.get("Grade"),  # 7
                            enh_lvl,  # 8
                            qual,  # 9
                            tier,  # 10
                            adv_reinf,  # 11
                            basic_eff_json,  # 12 👈 JSONB 데이터
                            add_eff_json,  # 13 👈 JSONB 데이터
                            ark_eff_raw,  # 14
                        )
                    )

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_eq_tuples:
                logger.info("적재할 장비 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_equipment_tb
                            (character_name, slot_index, type, name, collected_at, icon, grade,
                            honing_level, quality, item_tier, advanced_honing_level,
                            basic_effect, additional_effect, ark_passive_effect)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_eq_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 장비 데이터 총 {len(all_eq_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_gems(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 보석 데이터를 담을 큰 리스트
            all_gem_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item_data in extracted_data_list:
                char_name = item_data["character_name"]
                api_data = item_data["data"]

                gem_data = api_data.get("ArmoryGem")
                if not gem_data:
                    continue

                gems = gem_data.get("Gems", [])
                if not gems:
                    continue

                effects = gem_data.get("Effects", {})
                skills = effects.get("Skills", [])

                # 4. 개별 캐릭터 내에서 스킬 효과를 GemSlot 기준으로 딕셔너리화
                skill_dict = {}
                for s in skills:
                    slot = s.get("GemSlot")
                    desc_list = s.get("Description", [])
                    eff_type = desc_list[0] if desc_list else None

                    skill_dict[slot] = {
                        "skill_name": s.get("Name"),
                        "effect_option": s.get("Option"),
                        "effect_type": eff_type,
                    }

                # 5. 보석 리스트와 스킬 딕셔너리를 매핑하여 데이터 생성
                for g in gems:
                    slot = g.get("Slot")
                    matched_skill = skill_dict.get(slot, {})

                    # 💡 파싱 함수 적용 (미리 정의해두신 함수 사용)
                    eff_type_name, eff_type_val, basic_atk_val = parse_gem_effects(
                        matched_skill.get("effect_type"),
                        matched_skill.get("effect_option"),
                    )

                    all_gem_tuples.append(
                        (
                            char_name,
                            slot,
                            collected_at,
                            strip_html(g.get("Name")),
                            g.get("Grade"),
                            g.get("Level"),
                            matched_skill.get("skill_name"),
                            eff_type_name,
                            eff_type_val,
                            basic_atk_val,
                            g.get("Icon"),
                        )
                    )

            # 6. 적재할 데이터가 없으면 조기 종료
            if not all_gem_tuples:
                logger.info("적재할 보석 상세 데이터가 없습니다.")
                return

            # 7. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        # 💡 DELETE 없이 이력 누적 (INSERT)
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_gem_tb
                            (character_name, slot, collected_at, name, grade, level, skill_name,
                            effect_type_name, effect_type_value, basic_attack_boost_value, icon)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_gem_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 보석 상세 데이터 총 {len(all_gem_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        @task()
        def load_armory_profile(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 프로필 데이터를 담을 큰 리스트
            all_profile_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item_data in extracted_data_list:
                char_name = item_data["character_name"]
                api_data = item_data["data"]

                p = api_data.get("ArmoryProfile")
                if not p:
                    continue

                # 4. 개별 캐릭터 스탯/성향 딕셔너리 변환 (기존 로직 유지)
                stats_list = p.get("Stats", [])
                s_dict = {
                    s.get("Type"): clean_number(s.get("Value", 0)) for s in stats_list
                }

                tendencies_list = p.get("Tendencies", [])
                t_dict = {
                    t.get("Type"): int(t.get("Point", 0)) for t in tendencies_list
                }

                # 5. DB 적재용 파라미터 튜플 생성
                all_profile_tuples.append(
                    (
                        char_name,
                        collected_at,
                        p.get("ServerName"),
                        p.get("CharacterClassName"),
                        int(p.get("CharacterLevel", 0)),
                        clean_number(p.get("ItemAvgLevel")),
                        clean_number(p.get("CombatPower")),
                        p.get("CharacterImage"),
                        int(p.get("ExpeditionLevel", 0)),
                        p.get("TownLevel"),
                        p.get("TownName"),
                        p.get("Title"),
                        p.get("GuildMemberGrade"),
                        p.get("GuildName"),
                        p.get("UsingSkillPoint"),
                        p.get("TotalSkillPoint"),
                        p.get("HonorPoint"),
                        s_dict.get("공격력", 0),
                        s_dict.get("최대 생명력", 0),
                        s_dict.get("치명", 0),
                        s_dict.get("특화", 0),
                        s_dict.get("신속", 0),
                        s_dict.get("제압", 0),
                        s_dict.get("인내", 0),
                        s_dict.get("숙련", 0),
                        t_dict.get("지성", 0),
                        t_dict.get("담력", 0),
                        t_dict.get("매력", 0),
                        t_dict.get("친절", 0),
                    )
                )

            # 6. 적재할 데이터가 없으면 조기 종료
            if not all_profile_tuples:
                logger.info("적재할 프로필 데이터가 없습니다.")
                return

            # 7. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)  # 상단에 정의된 CONN_ID 사용
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        # 💡 execute -> executemany 로 변경됨
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_profile_tb (
                                character_name, collected_at, server_name, character_class_name, character_level,
                                item_avg_level, combat_power, character_image, expedition_level,
                                town_level, town_name, title, guild_member_grade, guild_name,
                                using_skill_point, total_skill_point, honor_point,
                                stat_atk, stat_hp, stat_crit, stat_spec, stat_swift, stat_dom, stat_end, stat_exp,
                                tend_intellect, tend_courage, tend_charm, tend_kindness
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s
                            );
                        """,
                            all_profile_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 프로필 통합 데이터 총 {len(all_profile_tuples)}건 (다수 캐릭터 분량) 시계열 이력 적재 완료!"
            )

        @task()
        def load_armory_skills(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                extracted_data_list = json.load(f)

            # 2. 전체 캐릭터의 스킬 데이터를 담을 큰 리스트
            all_skill_tuples = []

            # 3. 파일 내 전체 캐릭터 데이터 순회
            for item_data in extracted_data_list:
                char_name = item_data["character_name"]
                api_data = item_data["data"]

                skills = api_data.get("ArmorySkills", [])
                if not skills:
                    continue

                # 4. 개별 캐릭터의 스킬 리스트 순회
                for skill in skills:
                    s_name = skill.get("Name")
                    s_lvl = skill.get("Level")
                    tooltip_raw = skill.get("Tooltip")

                    # 💡 기존에 정의하신 파싱 함수 호출
                    pt = parse_skill_tooltip(tooltip_raw)

                    # 룬 정보 추출
                    rune = skill.get("Rune")
                    rune_name = rune.get("Name") if rune else None
                    rune_grade = rune.get("Grade") if rune else None

                    # 트라이포드 파싱 (선택된 것 위주)
                    tripods = skill.get("Tripods", [])
                    t_info = {
                        0: [None] * 3,
                        1: [None] * 3,
                        2: [None] * 3,
                    }  # Tier별 [name, icon, desc]

                    t1_name = None  # 필터링용 변수

                    for t in tripods:
                        if t.get("IsSelected"):
                            tier = t.get("Tier")
                            # 데이터 정리: [이름, 아이콘, 설명]
                            t_info[tier] = [
                                t.get("Name"),
                                t.get("Icon"),
                                strip_html(t.get("Tooltip", "")),
                            ]
                            if tier == 0:
                                t1_name = t.get("Name")

                    # 5. 1레벨 미사용 스킬 필터링 (기존 로직 유지)
                    if s_lvl == 1 and not rune_name and not t1_name:
                        continue

                    # 6. 튜플 생성 및 추가
                    all_skill_tuples.append(
                        (
                            char_name,
                            s_name,
                            collected_at,
                            s_lvl,
                            skill.get("Type"),
                            pt["cooldown"],
                            pt["mana_cost"],
                            pt["weak_point"],
                            pt["stagger"],
                            pt["attack_type"],
                            pt["is_counter"],
                            t_info[0][0],  # Tripod 1
                            t_info[1][0],  # Tripod 2
                            t_info[2][0],  # Tripod 3
                            rune_name,
                            rune_grade,
                            pt["rune_effect"],
                            # 💡 DB 타입에 맞게 JSONB 변환
                        )
                    )

            # 7. 적재할 데이터가 없으면 조기 종료
            if not all_skill_tuples:
                logger.info("적재할 스킬 데이터가 없습니다.")
                return

            # 8. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            conn = hook.get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO lostark.armory_skills_tb
                            (character_name, skill_name, collected_at, skill_level, type,
                            cooldown, mana_cost, weak_point, stagger, attack_type, is_counter,
                            tripod_1_name,
                            tripod_2_name,
                            tripod_3_name,
                            rune_name, rune_grade, rune_effect)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            all_skill_tuples,
                        )
                    conn.commit()
            finally:
                conn.close()

            logger.info(
                f"✅ 스킬 상세 데이터 총 {len(all_skill_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!"
            )

        load_ark_grid_cores(data_file_path)
        load_ark_grid_gems(data_file_path)
        load_armory_profile(data_file_path)
        load_armory_equipment(data_file_path)
        load_armory_skills(data_file_path)
        load_armory_gems(data_file_path)
        load_armory_engravings(data_file_path)
        load_armory_cards(data_file_path)
        load_armory_card_effects(data_file_path)
        load_armory_avatars(data_file_path)
        load_armory_collectibles(data_file_path)
        load_armory_collectible_details(data_file_path)
        load_ark_passive_points(data_file_path)
        load_ark_passive_effects(data_file_path)

    @task
    def generate_hourly_summary_report(**kwargs):
        # 1. PostgreSQL 연결 설정
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        target_schema = "lostark"

        # 2. Airflow Context에서 이번 배치의 1시간 구간(시간대) 가져오기
        # 예: 14:00 ~ 15:00 실행 건이라면 해당 시간이 할당됨
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        start_dt = now - timedelta(minutes=70)
        end_dt = now + timedelta(minutes=5)
        if start_dt == end_dt:
            start_dt = end_dt - timedelta(hours=1)
        start_time = start_dt.isoformat()
        end_time = end_dt.isoformat()
        # 3. 모니터링할 테이블 목록 (직접 지정하거나 DB에서 조회 가능)
        # 여기서는 예시로 딕셔너리 형태로 관리 (테이블명 리스트)
        base_table_for_char = "armory_profile_tb"
        char_column_name = "character_name"

        char_count_query = f"""
            SELECT COUNT(DISTINCT {char_column_name}) 
            FROM {target_schema}.{base_table_for_char}
            WHERE collected_at >= '{start_dt}' 
            AND collected_at < '{end_dt}';
        """

        try:
            char_count_result = pg_hook.get_first(char_count_query)
            collected_chars_count = char_count_result[0] if char_count_result else 0
        except Exception as e:
            collected_chars_count = "Error"
            logging.error(f"캐릭터 수 카운트 쿼리 실패: {str(e)}")

        # 3. 리포트 헤더 생성 (캐릭터 수 추가)
        report_lines = [
            f"⏳ **[Hourly Pipeline Report]**",
            f"⏱️ **Batch Window:** `{start_time}` ~ `{end_time}`",
            f"👤 **Collected Unique Characters:** `{collected_chars_count}` characters",
            "-" * 40,
        ]

        # 4. 모니터링할 테이블 목록 (현재 로그에 찍힌 테이블들 기준)
        target_tables = [
            "ark_grid_cores_tb",
            "ark_grid_gems_tb",
            "ark_passive_effects_tb",
            "ark_passive_points_tb",
            "armory_avatars_tb",
            "armory_card_effects_tb",
            "armory_card_tb",
            "armory_collectible_details_tb",
            "armory_collectibles_tb",
            "armory_engravings_tb",
            "armory_equipment_tb",
            "armory_gem_tb",
            "armory_profile_tb",
            "armory_skills_tb",
        ]

        total_hourly_inserted = 0

        # 5. 각 테이블별 통계 산출
        for table in target_tables:
            query = f"""
                WITH hourly_stats AS (
                    SELECT 
                        COUNT(*) AS hourly_inserted_rows,
                        MAX(collected_at) AS latest_collected_at
                    FROM {target_schema}.{table}
                    WHERE collected_at >= '{start_time}'::timestamptz 
                    AND collected_at < '{end_time}'::timestamptz
                ),
                table_size AS (
                    SELECT pg_size_pretty(pg_total_relation_size('{target_schema}.{table}')) AS total_size
                )
                SELECT 
                    h.hourly_inserted_rows, 
                    h.latest_collected_at, 
                    s.total_size 
                FROM hourly_stats h CROSS JOIN table_size s;
            """

            try:
                result = pg_hook.get_first(query)
                inserted_rows = result[0] or 0
                latest_time = result[1] or "No Data"
                total_size = result[2]

                total_hourly_inserted += inserted_rows

                status_icon = "🟢" if inserted_rows > 0 else "🔴"

                report_lines.append(f"{status_icon} **Table:** `{table}`")
                report_lines.append(f"   ├─ Inserted: {inserted_rows:,} rows")
                report_lines.append(f"   ├─ Latest Data: {latest_time}")
                report_lines.append(f"   └─ Total Size: {total_size}")

            except Exception as e:
                report_lines.append(f"⚠️ `{table}` 조회 실패: {str(e)}")

        report_lines.append("-" * 40)
        report_lines.append(
            f"✅ **Total Inserted in this hour:** {total_hourly_inserted:,} rows"
        )

        final_report = "\n".join(report_lines)
        logging.info("\n" + final_report)

        return final_report

    hourly_monitoring = generate_hourly_summary_report()
    data_file_path >> load_group >> hourly_monitoring


lostark_ark_passive_etl_dag()
