"""
Few-shot 예제 임베딩 생성 및 pgvector 적재.

질문을 마스킹(표준화) 처리한 후 OpenAI 임베딩을 생성하여
lostark.few_shot_examples_2 테이블에 적재합니다.

적재 테이블:
  - lostark.few_shot_examples_2
"""

import psycopg2
from pgvector.psycopg2 import register_vector
from openai import OpenAI

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OPENAI_API_KEY, get_db_conn_params

client = OpenAI(api_key=OPENAI_API_KEY)

# ──────────────────────────────────────────────
# 엔티티 마스킹 사전
# 길이가 긴 단어부터 치환되도록 정렬하여 오치환을 방지합니다.
# ──────────────────────────────────────────────
ENTITY_MAP = {
    # 캐릭터명
    "황로드유": "[CHARACTER_NAME]",
    "김뚜띠": "[CHARACTER_NAME]",
    "용기사쥐": "[CHARACTER_NAME]",
    "빅또루": "[CHARACTER_NAME]",
    # 등급
    "전설": "[GRADE_TYPE]",
    "유물": "[GRADE_TYPE]",
    "고대": "[GRADE_TYPE]",
    "에스더": "[GRADE_TYPE]",
    # 장신구 타입
    "목걸이": "[ACCESSORY_TYPE]",
    "귀걸이": "[ACCESSORY_TYPE]",
    "반지": "[ACCESSORY_TYPE]",
    # 각인명
    "결투의 대가": "[ENGRAVING_NAME]",
    "슈퍼 차지": "[ENGRAVING_NAME]",
    "안정된 상태": "[ENGRAVING_NAME]",
    "아드레날린": "[ENGRAVING_NAME]",
    "바리케이드": "[ENGRAVING_NAME]",
    "강령술": "[ENGRAVING_NAME]",
    "원한": "[ENGRAVING_NAME]",
    "각성": "[ENGRAVING_NAME]",
    # 직업 및 직업 각인
    "고독한 기사": "[EXCLUSIVE_ARK_PASSIVE_EFFECT_NAME]",
    "전투 태세": "[EXCLUSIVE_ARK_PASSIVE_EFFECT_NAME]",
    "워로드": "[CLASS_NAME]",
    # 아크패시브
    "도약": "[ARK_PASSIVE_CATEGORY]",
    "진화": "[ARK_PASSIVE_CATEGORY]",
    "깨달음": "[ARK_PASSIVE_CATEGORY]",
    "회심": "[ARK_PASSIVE_EFFECT_NAME]",
    "분쇄": "[ARK_PASSIVE_EFFECT_NAME]",
    "끝없는 마나": "[ARK_PASSIVE_EFFECT_NAME]",
    # 코어, 스킬, 트라이포드, 룬
    "현란한 무기": "[CORE_NAME]",
    "방패 돌진": "[SKILL_NAME]",
    "배쉬": "[SKILL_NAME]",
    "버스트 캐넌": "[SKILL_NAME]",
    "약점 포착": "[TRIPOD_NAME]",
    "단죄": "[RUNE_NAME]",
    "질풍": "[RUNE_NAME]",
}

SORTED_ENTITY_KEYS = sorted(ENTITY_MAP.keys(), key=len, reverse=True)


def mask_question(text: str) -> str:
    """사용자 질문에서 특정 엔티티를 표준화된 태그로 치환합니다."""
    masked = text
    for key in SORTED_ENTITY_KEYS:
        masked = masked.replace(key, ENTITY_MAP[key])
    return masked


def get_embedding(text: str) -> list[float]:
    """OpenAI text-embedding-3-small 모델로 1536차원 벡터를 생성합니다."""
    response = client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding


# ──────────────────────────────────────────────
# Few-shot 예제 데이터
# 주석 처리된 예제를 활성화하여 사용하세요.
# ──────────────────────────────────────────────
FEW_SHOT_EXAMPLES: list[dict] = [
    # {
    #     "question": "용기사쥐 최근에 각인 바꾼 거 있어?",
    #     "question_category": "ENGRAVING",
    #     "analysis_type": "COMPARE",
    #     "explanation": "가장 최근(curr)과 직전(prev) 두 시점의 스냅샷을 비교...",
    #     "sql_query": "WITH time_refs AS (...) SELECT ...",
    # },
    # 실제 예제 데이터는 이 리스트에 추가하세요.
]


def run():
    """Few-shot 예제를 임베딩하고 pgvector 테이블에 적재합니다."""
    if not FEW_SHOT_EXAMPLES:
        print("적재할 few-shot 예제가 없습니다. FEW_SHOT_EXAMPLES 리스트를 채워주세요.")
        return

    conn = psycopg2.connect(**get_db_conn_params())
    cursor = conn.cursor()

    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    register_vector(conn)

    create_table_query = """
    CREATE TABLE IF NOT EXISTS lostark.few_shot_examples_2 (
        id SERIAL PRIMARY KEY,
        question TEXT NOT NULL,
        question_category VARCHAR(50),
        analysis_type VARCHAR(50),
        explanation TEXT,
        sql_query TEXT NOT NULL,
        embedding vector(1536)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    print(f"총 {len(FEW_SHOT_EXAMPLES)}개의 few-shot 데이터를 적재합니다...")

    for i, example in enumerate(FEW_SHOT_EXAMPLES):
        raw_q = example["question"]
        question_category = example.get("question_category", "TOTAL_INFO")
        analysis_type = example.get("analysis_type", "")
        explanation = example.get("explanation", "")
        sql_query = example["sql_query"]

        masked_q = mask_question(raw_q)
        emb_vector = get_embedding(masked_q)

        insert_query = """
        INSERT INTO lostark.few_shot_examples_2
            (question, question_category, analysis_type, explanation, sql_query, embedding)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            raw_q, question_category, analysis_type,
            explanation, sql_query, emb_vector,
        ))

        print(f"  [{i+1}/{len(FEW_SHOT_EXAMPLES)}] [{question_category}] {raw_q}")

    conn.commit()
    cursor.close()
    conn.close()
    print("모든 few-shot 데이터 적재 완료!")


if __name__ == "__main__":
    run()
