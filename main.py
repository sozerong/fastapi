from fastapi import FastAPI, Query, Body
from pydantic import BaseModel
from typing import List
from fastapi.middleware.cors import CORSMiddleware

import os
from dotenv import load_dotenv
import json
from urllib.parse import urlparse

# ✅ 환경 변수 로드
load_dotenv()

# ✅ PostgreSQL 설정
db_url = os.getenv("DATABASE_URL")
parsed_url = urlparse(db_url)

PG_DB = parsed_url.path[1:]
PG_USER = parsed_url.username
PG_PASSWORD = parsed_url.password
PG_HOST = parsed_url.hostname
PG_PORT = parsed_url.port

conn = psycopg2.connect(
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT
)
conn.autocommit = True


# ✅ FastAPI 앱 생성
app = FastAPI()

# ✅ CORS 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # 필요시 * 또는 배포 주소로 변경
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ 데이터 저장 API
@app.post("/save")
def save_data(payload: List[dict] = Body(...)):
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS graphrag_answers;")
        cursor.execute("""
            CREATE TABLE graphrag_answers (
                id SERIAL PRIMARY KEY,
                question TEXT,
                recommendations JSONB,
                keywords TEXT[]
            );
        """)
        conn.commit()
        print("✅ 테이블 생성 완료")

        for doc in payload:
            question = doc.get("question", "")
            try:
                parsed_answer = doc["answer"] if isinstance(doc["answer"], list) else json.loads(doc["answer"])
            except Exception:
                print(f"⚠ JSON 파싱 실패: {question}")
                parsed_answer = [{"name": "[FORMAT ERROR]", "description": str(doc.get('answer'))}]
            keywords = doc.get("keywords", [])
            cursor.execute(
                "INSERT INTO graphrag_answers (question, recommendations, keywords) VALUES (%s, %s, %s);",
                (question, json.dumps(parsed_answer), keywords)
            )
        conn.commit()
        print("✅ 데이터 저장 완료")

    return {"status": "success", "count": len(payload)}
