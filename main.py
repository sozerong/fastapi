from fastapi import FastAPI, Query, Body
from pydantic import BaseModel
from typing import List
from fastapi.middleware.cors import CORSMiddleware

from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import json
from urllib.parse import urlparse

# ✅ 환경 변수 로드
load_dotenv("configs/.env")

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

# ✅ Neo4j 설정
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

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


# ✅ PostgreSQL 검색 API
@app.get("/search")
def search_answers(query: str = Query(..., description="질문 키워드 또는 문장")):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT question, recommendations, keywords
            FROM graphrag_answers
            WHERE question ILIKE %s
            ORDER BY id
            LIMIT 5
        """, (f"%{query}%",))
        results = cur.fetchall()
    return results


# ✅ Neo4j 지식그래프 API
@app.get("/graph")
def get_graph(keyword: str = Query(None), all: bool = Query(False)):
    def full_graph(tx):
        query = """
                MATCH (n)-[r]->(m)
                RETURN n, r, m
                LIMIT 600
                """
        result = tx.run(query)

        nodes = {}
        edges = set()

        for record in result:
            n = record["n"]
            m = record["m"]
            r = record["r"]

            nid = n.get("name") or n.get("value")
            mid = m.get("name") or m.get("value")
            if not nid or not mid:
                continue

            ntype = list(n.labels)[0]
            mtype = list(m.labels)[0]

            nodes[nid] = {"id": nid, "type": ntype}
            nodes[mid] = {"id": mid, "type": mtype}
            edges.add((nid, mid, r.type))

        return {
            "nodes": list(nodes.values()),
            "edges": [{"source": s, "target": t, "label": l} for s, t, l in edges]
        }

    def keyword_graph(tx):
        # 기존 keyword 기반 그래프 탐색 쿼리 구현
        ...

    with driver.session() as session:
        if all:
            return session.execute_read(full_graph)
        elif keyword:
            return session.execute_read(keyword_graph)
        else:
            return {"error": "keyword 또는 all 파라미터가 필요합니다."}
