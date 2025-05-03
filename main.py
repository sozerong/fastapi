from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase
from urllib.parse import urlparse

# ✅ 환경변수 로드
load_dotenv("configs/.env")

# ✅ FastAPI 앱 생성
app = FastAPI()

# ✅ CORS 허용 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://capstone-app-mu.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ PostgreSQL (A) 연결: 키워드 추천용 DB
db_url_main = os.getenv("DATABASE_URL")
parsed_url = urlparse(db_url_main)
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

# ✅ Neo4j 연결
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ✅ PostgreSQL (B) 연결: 서울시 매출 요약용 DB
db_url_sales = os.getenv("SALES_DATABASE_URL")  # 💡 .env에 따로 정의해두세요
if not db_url_sales:
    raise RuntimeError("❌ SALES_DATABASE_URL 환경변수를 찾을 수 없습니다.")
sales_engine = create_engine(db_url_sales)

# ✅ 1. 키워드 기반 추천
@app.get("/search")
def search_answers(query: str = Query(...)):
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

# ✅ 2. 전체 지식그래프 반환
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
            nodes[nid] = {"id": nid, "type": list(n.labels)[0]}
            nodes[mid] = {"id": mid, "type": list(m.labels)[0]}
            edges.add((nid, mid, r.type))
        return {
            "nodes": list(nodes.values()),
            "edges": [{"source": s, "target": t, "label": l} for s, t, l in edges]
        }

    def keyword_graph(tx):
        return {"message": f"'{keyword}'에 대한 그래프는 아직 구현되지 않음."}

    with driver.session() as session:
        if all:
            return session.execute_read(full_graph)
        elif keyword:
            return session.execute_read(keyword_graph)
        else:
            return {"error": "keyword 또는 all 파라미터가 필요합니다."}

# ✅ 3. 자치구별 매출 전체 요약
@app.get("/sales/{gu_name}")
def get_sales_summary(gu_name: str):
    query = "SELECT * FROM seoul_sales_summary WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return df.iloc[0].to_dict()

# ✅ 4. 자치구별 카페당 월 평균 매출만
@app.get("/sales/monthly_avg/{gu_name}")
def get_cafe_monthly_avg(gu_name: str):
    query = "SELECT 자치구, 카페당_월_평균_매출 FROM seoul_sales_summary WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return {
        "자치구": df.iloc[0]["자치구"],
        "카페당_월_평균_매출": int(df.iloc[0]["카페당_월_평균_매출"])
    }
