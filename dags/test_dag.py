import logging
from datetime import datetime, timezone
import os
import time
import json
from airflow.decorators import dag, task  # 최신 방식의 데코레이터 사용
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# 💡 수정됨: 'plugins.' 생략 및 parsers 임포트 하나로 병합
from extractors import fetch_armory_data, fetch_sibling_characters
from parsers import (
    parse_tooltip_content, split_core_options, split_gem_effect, strip_html,
    parse_ark_passive_description, parse_rank_level, parse_avatar_tooltip,
    extract_card_description, parse_equipment_tooltip, parse_gem_effects,
    clean_number, parse_skill_tooltip, parse_additional_effect_to_json,
    parse_basic_effect_to_json
)
from alerts import discord_failure_callback

logger = logging.getLogger(__name__)

CONN_ID = "postgres_lostark"

@dag(
    dag_id="chatbot_response_processor",  # FastAPI에서 호출하는 ID와 일치시킴
    schedule=None,  # 정기 실행 없음 (API 전용)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lostark", "api", "on-demand"],
    default_args={
        "on_failure_callback": discord_failure_callback,
        "retries": 0,
    },
)
def lostark_single_character_etl():

    @task
    def extract_character_data(dag_run=None, **context) -> str:
        # 1. FastAPI에서 보낸 데이터 읽기
        conf = dag_run.conf if dag_run else {}
        char_name = conf.get('character_name')
        
        if not char_name:
            raise ValueError("❌ 전달받은 캐릭터 이름(character_name)이 없습니다.")

        api_key = Variable.get("LOSTARK_API_KEY") 
        run_id_str = context['logical_date'].strftime("%Y%m%d_%H%M%S")
        file_path = f"/tmp/on_demand_{char_name}_{run_id_str}.json"

        logging.info(f"🚀 [{char_name}] 캐릭터 API 호출 시작 (Source: {conf.get('request_source')})")

        # 2. 로스트아크 API 호출 (딱 한 명만)
        try:
            raw_data = fetch_armory_data(char_name, api_key)
            if not raw_data:
                raise Exception(f"API 응답이 비어있습니다. (이름 확인 필요: {char_name})")
            
            # 기존 로직과 호환성을 위해 리스트 형태로 저장
            extracted_results = [{
                "character_name": char_name,
                "data": raw_data
            }]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(extracted_results, f, ensure_ascii=False)
                
            logging.info(f"✅ 데이터 저장 완료: {file_path}")
            return file_path

        except Exception as e:
            logging.error(f"❌ {char_name} 호출 실패: {e}")
            raise

    # 파일 경로를 받아오는 태스크 실행
    data_file_path = extract_character_data()

    
    with TaskGroup(group_id='load_tasks', tooltip='데이터베이스 적재 태스크 그룹') as load_group:

        @task()
        def load_character_siblings(dag_run=None, **context):
            """원정대(siblings) API 호출 → character_info_tb UPSERT"""
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            conf = dag_run.conf if dag_run else {}
            char_name = conf.get('character_name')
            if not char_name:
                raise ValueError("❌ 전달받은 캐릭터 이름(character_name)이 없습니다.")

            api_key = Variable.get("LOSTARK_API_KEY")

            logger.info(f"[{char_name}] 원정대 정보 조회 중...")
            siblings_data = fetch_sibling_characters(char_name, api_key)

            if not siblings_data:
                logger.info("적재할 원정대 데이터가 없습니다.")
                return

            # 데이터 전처리: "1,683.00" -> 1683.00
            processed_tuples = []
            for char in siblings_data:
                raw_item_level = char.get("ItemAvgLevel") or "0"
                clean_item_level = float(raw_item_level.replace(",", ""))

                processed_tuples.append((
                    char.get("CharacterName"),
                    char.get("ServerName"),
                    char.get("CharacterLevel"),
                    char.get("CharacterClassName"),
                    clean_item_level,
                ))

            # DB 적재 (UPSERT)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.character_info_tb
                        (character_name, server_name, character_level, character_class_name, item_avg_level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (character_name) DO UPDATE SET
                            server_name = EXCLUDED.server_name,
                            character_level = EXCLUDED.character_level,
                            character_class_name = EXCLUDED.character_class_name,
                            item_avg_level = EXCLUDED.item_avg_level;
                    """, processed_tuples)
                conn.commit()

            logger.info(f"✅ 원정대 캐릭터 {len(processed_tuples)}건 전처리 및 UPSERT 완료")

        @task()
        def load_ark_grid_cores(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")
            if not os.path.exists(file_path):
                logging.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return
                
            with open(file_path, 'r', encoding='utf-8') as f:
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
                    core_opt_raw = parse_tooltip_content(slot.get("Tooltip"), "코어 옵션")
                    # 2차 파싱 (레벨별 딕셔너리로 분할)
                    opts = split_core_options(core_opt_raw)
                    
                    all_core_tuples.append((
                        char_name, core_idx, collected_at, 
                        slot.get("Name"), slot.get("Grade"), slot.get("Point"), slot.get("Icon"),
                        opts['p1'], opts['o1'], opts['p2'], opts['o2'],
                        opts['p3'], opts['o3'], opts['p4'], opts['o4'],
                        opts['p5'], opts['o5'], opts['p6'], opts['o6']
                    ))

            if not all_core_tuples:
                return

            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.ark_grid_cores_tb 
                        (
                            character_name, slot_index, collected_at, name, grade, point, icon, 
                            level_1_point, level_1_option, level_2_point, level_2_option, 
                            level_3_point, level_3_option, level_4_point, level_4_option, 
                            level_5_point, level_5_option, level_6_point, level_6_option
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_core_tuples)
                conn.commit()
            logger.info(f"코어 데이터({len(all_core_tuples)}건) 컬럼 분할 및 이력 적재 완료!")

        @task()
        def load_ark_grid_gems(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 전달받은 파일 경로의 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                        gem_eff_raw = parse_tooltip_content(gem.get("Tooltip"), "젬 효과")
                        
                        # 2차 파싱: 텍스트 덩어리를 9개의 속성으로 분해
                        opts = split_gem_effect(gem_eff_raw)
                        
                        # 총 17개 매핑하여 큰 리스트에 추가
                        all_gem_tuples.append((
                            char_name, core_idx, gem.get("Index"), collected_at, 
                            gem.get("Grade"), gem.get("IsActive"), gem.get("Icon"),
                            opts['req_will'], opts['will_eff'], opts['pt_type'], opts['pt_val'],
                            opts['e1_name'], opts['e1_lvl'], opts['e1_val'],
                            opts['e2_name'], opts['e2_lvl'], opts['e2_val']
                        ))

            # 4. 적재할 데이터가 아예 없다면 조기 종료
            if not all_gem_tuples:
                logger.info("적재할 젬 데이터가 없습니다.")
                return

            # 5. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.ark_grid_gems_tb 
                        (
                            character_name, core_index, gem_index, collected_at, grade, is_active, icon,
                            required_willpower, willpower_efficiency, point_type, point_value,
                            effect_1_name, effect_1_level, effect_1_value,
                            effect_2_name, effect_2_level, effect_2_value
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_gem_tuples)
                conn.commit()
                
            logger.info(f"✅ 젬 데이터 총 {len(all_gem_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")

        @task()
        def load_ark_passive_points(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 JSON 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                    
                    all_point_tuples.append((
                        char_name, 
                        p.get("Name"), 
                        collected_at, 
                        p.get("Value"), 
                        rank_val, 
                        level_val
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_point_tuples:
                logger.info("적재할 패시브 포인트 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (executemany)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # 💡 DELETE/UPSERT 없이 INSERT 문으로 이력 누적
                    cur.executemany("""
                        INSERT INTO lostark.ark_passive_points_tb 
                        (character_name, name, collected_at, value, point_rank, point_level)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, all_point_tuples)
                conn.commit()
                
            logger.info(f"✅ 포인트 데이터 총 {len(all_point_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")

        @task()
        def load_ark_passive_effects(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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

                    all_effect_tuples.append((
                        char_name, 
                        e.get("Name"), 
                        collected_at, 
                        e.get("Icon"), 
                        tier, 
                        effect_name, 
                        level
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_effect_tuples:
                logger.info("적재할 패시브 효과 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.ark_passive_effects_tb 
                        (character_name, name, collected_at, icon, tier, effect_name, level)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, all_effect_tuples)
                conn.commit()
                
            logger.info(f"✅ 패시브 효과 데이터 총 {len(all_effect_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")       

        @task()
        def load_armory_avatars(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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

                    all_av_tuples.append((
                        char_name,
                        i.get("Name"),
                        collected_at,
                        i.get("Type"),
                        i.get("Icon"),
                        i.get("Grade"),
                        i.get("IsSet"),
                        i.get("IsInner"),
                        opts.get("basic_stat"),       # 예: '힘'
                        opts.get("basic_val"),        # 예: 2.00
                        opts.get("intellect"),        # 예: 5
                        opts.get("courage"),          # 예: 5
                        opts.get("charm"),            # 예: 5
                        opts.get("kindness"),         # 예: 5
                        opts.get("source")
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_av_tuples:
                logger.info("적재할 아바타 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # 💡 DELETE 없이 시간대별 이력 적재
                    cur.executemany("""
                        INSERT INTO lostark.armory_avatars_tb 
                        (
                            character_name, name, collected_at, type, icon, grade, is_set, is_inner, 
                            basic_effect_stat, basic_effect_value, 
                            tendency_intellect, tendency_courage, tendency_charm, tendency_kindness, 
                            source
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_av_tuples)
                conn.commit()
                    
            logger.info(f"✅ 아바타 데이터 총 {len(all_av_tuples)}건 (다수 캐릭터 분량) 스탯 분할 및 일괄 적재 완료!")

        @task()
        def load_armory_cards(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                    
                    all_card_tuples.append((
                        char_name,
                        c.get("Slot"),
                        collected_at,
                        c.get("Name"),
                        c.get("Icon"),
                        c.get("Grade"),
                        c.get("AwakeCount", 0),
                        c.get("AwakeTotal", 0),
                        desc
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_card_tuples:
                logger.info("적재할 카드 장착 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # 💡 DELETE 없이 시간대별 이력 적재
                    cur.executemany("""
                        INSERT INTO lostark.armory_card_tb 
                        (character_name, slot, collected_at, name, icon, grade, awake_count, awake_total, description) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_card_tuples)
                conn.commit()
                    
            logger.info(f"✅ 카드 장착 데이터 총 {len(all_card_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")
        
        @task()
        def load_armory_card_effects(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                        all_effect_tuples.append((
                            char_name,
                            item.get("Name"),
                            collected_at,
                            item.get("Description")
                        ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_effect_tuples:
                logger.info("적재할 카드 세트 효과 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.armory_card_effects_tb 
                        (character_name, effect_name, collected_at, description) 
                        VALUES (%s, %s, %s, %s);
                    """, all_effect_tuples)
                conn.commit()
                    
            logger.info(f"✅ 카드 세트 효과 데이터 총 {len(all_effect_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")

        @task()
        def load_armory_collectibles(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                    all_summary_tuples.append((
                        char_name,
                        i.get("Type"),
                        collected_at,
                        i.get("Icon"),
                        i.get("Point"),
                        i.get("MaxPoint")
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_summary_tuples:
                logger.info("적재할 수집형 포인트 요약 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.armory_collectibles_tb 
                        (character_name, type, collected_at, icon, point, max_point) 
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, all_summary_tuples)
                conn.commit()
                    
            logger.info(f"✅ 수집형 포인트 요약 데이터 총 {len(all_summary_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")

        @task()
        def load_armory_collectible_details(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                        all_detail_tuples.append((
                            char_name,
                            c_type,
                            cp.get("PointName"),
                            collected_at,
                            cp.get("Point"),
                            cp.get("MaxPoint")
                        ))

            # 6. 적재할 데이터가 없으면 조기 종료
            if not all_detail_tuples:
                logger.info("적재할 수집형 포인트 상세 데이터가 없습니다.")
                return

            # 7. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.armory_collectible_details_tb 
                        (character_name, type, point_name, collected_at, point, max_point) 
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, all_detail_tuples)
                conn.commit()
                    
            logger.info(f"✅ 수집형 포인트 상세 데이터 총 {len(all_detail_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")

        @task()
        def load_armory_engravings(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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

                    all_engraving_tuples.append((
                        char_name,
                        effect.get("Name"),
                        collected_at,
                        effect.get("Grade"),
                        effect.get("Level"),
                        effect.get("AbilityStoneLevel"), # 값이 없으면 자동으로 None(NULL) 처리됨
                        clean_desc
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_engraving_tuples:
                logger.info("적재할 각인 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # 💡 DELETE 없이 시간대별로 이력 계속 적재 (INSERT)
                    cur.executemany("""
                        INSERT INTO lostark.armory_engravings_tb 
                        (character_name, name, collected_at, grade, level, ability_stone_level, description) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, all_engraving_tuples)
                conn.commit()
                    
            logger.info(f"✅ 각인 데이터 총 {len(all_engraving_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")
        
        @task()
        def load_armory_equipment(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                        basic_eff_raw,   # 👈 텍스트 원본
                        add_eff_raw,     # 👈 텍스트 원본
                        ark_eff_raw, 
                        adv_reinf
                    ) = parse_equipment_tooltip(i.get("Tooltip"), item_name)
                    
                    basic_eff_json = parse_basic_effect_to_json(basic_eff_raw)
                    add_eff_json = parse_additional_effect_to_json(item_type, add_eff_raw)
                    
                    all_eq_tuples.append((
                        char_name,               # 1
                        idx,                     # 2
                        item_type,               # 3
                        item_name,               # 4
                        collected_at,            # 5
                        i.get("Icon"),           # 6
                        i.get("Grade"),          # 7
                        enh_lvl,                 # 8
                        qual,                    # 9
                        tier,                    # 10
                        adv_reinf,               # 11
                        basic_eff_json,          # 12 👈 JSONB 데이터
                        add_eff_json,            # 13 👈 JSONB 데이터
                        ark_eff_raw              # 14
                    ))

            # 5. 적재할 데이터가 없으면 조기 종료
            if not all_eq_tuples:
                logger.info("적재할 장비 데이터가 없습니다.")
                return

            # 6. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.armory_equipment_tb 
                        (character_name, slot_index, type, name, collected_at, icon, grade, 
                        honing_level, quality, item_tier, advanced_honing_level,
                        basic_effect, additional_effect, ark_passive_effect) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_eq_tuples)
                conn.commit()
                    
            logger.info(f"✅ 장비 데이터 총 {len(all_eq_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")       
        
        @task()
        def load_armory_gems(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                    (eff_type_name, eff_type_val, basic_atk_val) = parse_gem_effects(
                        matched_skill.get("effect_type"), 
                        matched_skill.get("effect_option")
                    )

                    all_gem_tuples.append((
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
                        g.get("Icon")
                    ))

            # 6. 적재할 데이터가 없으면 조기 종료
            if not all_gem_tuples:
                logger.info("적재할 보석 상세 데이터가 없습니다.")
                return

            # 7. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # 💡 DELETE 없이 이력 누적 (INSERT)
                    cur.executemany("""
                        INSERT INTO lostark.armory_gem_tb 
                        (character_name, slot, collected_at, name, grade, level, skill_name, 
                        effect_type_name, effect_type_value, basic_attack_boost_value, icon) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_gem_tuples)
                conn.commit()
                    
            logger.info(f"✅ 보석 상세 데이터 총 {len(all_gem_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")

        @task()
        def load_armory_profile(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                s_dict = {s.get("Type"): clean_number(s.get("Value", 0)) for s in stats_list}

                tendencies_list = p.get("Tendencies", [])
                t_dict = {t.get("Type"): int(t.get("Point", 0)) for t in tendencies_list}

                # 5. DB 적재용 파라미터 튜플 생성
                all_profile_tuples.append((
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
                ))

            # 6. 적재할 데이터가 없으면 조기 종료
            if not all_profile_tuples:
                logger.info("적재할 프로필 데이터가 없습니다.")
                return

            # 7. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID) # 상단에 정의된 CONN_ID 사용
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # 💡 execute -> executemany 로 변경됨
                    cur.executemany("""
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
                    """, all_profile_tuples)
                conn.commit()
                    
            logger.info(f"✅ 프로필 통합 데이터 총 {len(all_profile_tuples)}건 (다수 캐릭터 분량) 시계열 이력 적재 완료!")
        
        @task()
        def load_armory_skills(file_path: str, **context):
            collected_at = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")

            # 1. 파일 존재 여부 확인 및 데이터 로드
            if not os.path.exists(file_path):
                logger.error(f"데이터 파일을 찾을 수 없습니다: {file_path}")
                return

            with open(file_path, 'r', encoding='utf-8') as f:
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
                    t_info = {0: [None]*3, 1: [None]*3, 2: [None]*3} # Tier별 [name, icon, desc]
                    
                    t1_name = None # 필터링용 변수
                    
                    for t in tripods:
                        if t.get("IsSelected"):
                            tier = t.get("Tier")
                            # 데이터 정리: [이름, 아이콘, 설명]
                            t_info[tier] = [
                                t.get("Name"),
                                t.get("Icon"),
                                strip_html(t.get("Tooltip", ""))
                            ]
                            if tier == 0: t1_name = t.get("Name")

                    # 5. 1레벨 미사용 스킬 필터링 (기존 로직 유지)
                    if s_lvl == 1 and not rune_name and not t1_name:
                        continue

                    # 6. 튜플 생성 및 추가
                    all_skill_tuples.append((
                        char_name, s_name, collected_at, s_lvl, 
                        skill.get("Type"), 
                        pt["cooldown"], pt["mana_cost"], pt["weak_point"], 
                        pt["stagger"], pt["attack_type"], pt["is_counter"],
                        t_info[0][0], # Tripod 1
                        t_info[1][0],  # Tripod 2
                        t_info[2][0], # Tripod 3
                        rune_name, rune_grade, pt["rune_effect"]
                       # 💡 DB 타입에 맞게 JSONB 변환
                    ))

            # 7. 적재할 데이터가 없으면 조기 종료
            if not all_skill_tuples:
                logger.info("적재할 스킬 데이터가 없습니다.")
                return

            # 8. DB 연결 및 일괄 적재 (Batch Insert)
            hook = PostgresHook(postgres_conn_id=CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO lostark.armory_skills_tb 
                        (character_name, skill_name, collected_at, skill_level, type, 
                        cooldown, mana_cost, weak_point, stagger, attack_type, is_counter,
                        tripod_1_name,
                        tripod_2_name,
                        tripod_3_name,
                        rune_name, rune_grade, rune_effect) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, all_skill_tuples)
                conn.commit()
                    
            logger.info(f"✅ 스킬 상세 데이터 총 {len(all_skill_tuples)}건 (다수 캐릭터 분량) 일괄 적재 완료!")


        load_character_siblings()
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




    # 태스크 순서 정의
    data_file_path >> load_group

lostark_single_character_etl()