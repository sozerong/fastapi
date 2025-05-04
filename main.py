from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse

# ✅ 환경변수 로딩
load_dotenv()

app = FastAPI()

# ✅ CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://capstone-app-mu.vercel.app",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ DB (A): 키워드 추천용
db_url_main = os.getenv("DATABASE_URL")
if not db_url_main:
    raise RuntimeError("❌ DATABASE_URL 환경변수를 찾을 수 없습니다.")
parsed_url = urlparse(db_url_main)
conn = psycopg2.connect(
    dbname=parsed_url.path[1:],
    user=parsed_url.username,
    password=parsed_url.password,
    host=parsed_url.hostname,
    port=parsed_url.port
)
conn.autocommit = True

# ✅ DB (B): 매출/카페 데이터용
db_url_sales = os.getenv("SALES_DATABASE_URL")
if not db_url_sales:
    raise RuntimeError("❌ SALES_DATABASE_URL 환경변수를 찾을 수 없습니다.")
sales_engine = create_engine(db_url_sales)

# ✅ 0. 헬스체크
@app.get("/")
def read_root():
    return {"message": "FastAPI is running"}

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

# ✅ 2. 전체 지식그래프 (Neo4j 제거 → 단순 텍스트 응답)
@app.get("/graph")
def get_graph(keyword: str = Query(None), all: bool = Query(False)):
    return {"message": "neo4j"}

# ✅ 3. 자치구 전체 항목 요약 조회
@app.get("/sales/{gu_name}")
def get_sales_summary(gu_name: str):
    query = "SELECT * FROM seoul_sales_summary WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return df.iloc[0].to_dict()

# ✅ 4. 자치구별 카페당 월 평균 매출
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

# ✅ 5. 자치구별 인구수당 카페 비율
@app.get("/districts/cafe_ratio/{gu_name}")
def get_district_cafe_ratio_by_gu(gu_name: str):
    query = "SELECT * FROM district_cafe_ratio WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return df.iloc[0].to_dict()

# ✅ 6. 메뉴별 평균 가격 비교
@app.get("/menu/price_stats/{gu_name}")
def get_menu_price_stats_by_gu(gu_name: str):
    query = "SELECT * FROM menu_price_stats WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구 데이터가 없습니다."}
    return df.to_dict(orient="records")

# ✅ 7. 인기 메뉴 리스트
@app.get("/menu/popular/{gu_name}")
def get_popular_menu_by_gu(gu_name: str):
    query = "SELECT * FROM popular_menu WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구의 인기 메뉴 데이터가 없습니다."}
    return df.to_dict(orient="records")

# ✅ 8. 자치구별 개인/프랜차이즈 카페 수
@app.get("/districts/cafe_count/{gu_name}")
def get_district_cafe_count(gu_name: str):
    query = "SELECT * FROM district_cafe_count WHERE 자치구 = %s"
    df = pd.read_sql(query, sales_engine, params=(gu_name,))
    if df.empty:
        return {"error": f"{gu_name} 자치구의 카페 수 데이터가 없습니다."}
    return df.iloc[0].to_dict()
