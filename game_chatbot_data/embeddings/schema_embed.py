"""
DB 스키마 메타데이터(테이블/칼럼 코멘트) 임베딩 및 pgvector 적재.

PostgreSQL의 pg_class, pg_attribute에서 코멘트를 추출한 후
OpenAI 임베딩을 생성하여 lostark.schema_comments_tb에 적재합니다.

적재 테이블:
  - lostark.schema_comments_tb
"""

import psycopg2
from pgvector.psycopg2 import register_vector
from openai import OpenAI

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OPENAI_API_KEY, get_db_conn_params, DB_SCHEMA

client = OpenAI(api_key=OPENAI_API_KEY)


def get_embedding(text: str) -> list[float]:
    """OpenAI text-embedding-3-small 모델로 1536차원 벡터를 생성합니다."""
    response = client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding


def run():
    """스키마 메타데이터를 추출하고 임베딩하여 적재합니다."""
    conn = psycopg2.connect(**get_db_conn_params())
    cursor = conn.cursor()
    register_vector(conn)

    print(f"[1/3] '{DB_SCHEMA}' 스키마에서 테이블/칼럼 코멘트 추출 중...")

    # 테이블 코멘트 추출
    cursor.execute(f"""
        SELECT n.nspname, c.relname, obj_description(c.oid, 'pg_class')
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{DB_SCHEMA}'
          AND c.relkind = 'r'
          AND obj_description(c.oid, 'pg_class') IS NOT NULL;
    """)
    table_comments = cursor.fetchall()

    # 칼럼 코멘트 추출
    cursor.execute(f"""
        SELECT n.nspname, c.relname, a.attname, col_description(c.oid, a.attnum)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON c.oid = a.attrelid
        WHERE n.nspname = '{DB_SCHEMA}'
          AND c.relkind = 'r'
          AND a.attnum > 0 AND NOT a.attisdropped
          AND col_description(c.oid, a.attnum) IS NOT NULL;
    """)
    column_comments = cursor.fetchall()

    # 통합 리스트 생성
    all_metadata = []
    for row in table_comments:
        all_metadata.append({
            "schema_name": row[0], "table_name": row[1],
            "column_name": None, "comment_type": "TABLE", "comment_text": row[2],
        })
    for row in column_comments:
        all_metadata.append({
            "schema_name": row[0], "table_name": row[1],
            "column_name": row[2], "comment_type": "COLUMN", "comment_text": row[3],
        })

    print(f"  테이블 {len(table_comments)}개, 칼럼 {len(column_comments)}개 추출 완료")
    print(f"[2/3] 임베딩 생성 및 적재 시작 (총 {len(all_metadata)}건)...")

    insert_query = """
        INSERT INTO lostark.schema_comments_tb
        (schema_name, table_name, column_name, comment_type, comment_text, comment_embedding)
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    for i, meta in enumerate(all_metadata):
        if meta["comment_type"] == "TABLE":
            embedding_input = f"테이블명: {meta['table_name']}, 설명: {meta['comment_text']}"
        else:
            embedding_input = f"테이블명: {meta['table_name']}, 칼럼명: {meta['column_name']}, 설명: {meta['comment_text']}"

        emb_vector = get_embedding(embedding_input)

        cursor.execute(insert_query, (
            meta["schema_name"], meta["table_name"], meta["column_name"],
            meta["comment_type"], meta["comment_text"], emb_vector,
        ))

        label = f"({meta['column_name']})" if meta["column_name"] else ""
        print(f"  [{i+1}/{len(all_metadata)}] [{meta['comment_type']}] {meta['table_name']} {label}")

    conn.commit()
    cursor.close()
    conn.close()
    print("[3/3] 스키마 메타데이터 벡터 적재 완료!")


if __name__ == "__main__":
    run()
