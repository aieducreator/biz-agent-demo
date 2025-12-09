### 1. 필요한 라이브러리 / 모듈 / 함수 임포트
import os
import json
import asyncio
import uuid
import psycopg2
import decimal
from typing import List, Dict, Any, Annotated
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.checkpoint.memory import MemorySaver

### 2. 환경 설정

# 실행 파일 폴더 경로 가져오기
folder_path = os.path.dirname(os.path.abspath(__file__))

# 환경변수 로드
env_file_path = os.path.join(folder_path, '.env')
load_dotenv(env_file_path)

# llm 생성하기
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

### 3. LangGraph 상태 정의 
class AnalysisState(BaseModel):
    messages: Annotated[List[BaseMessage], add_messages]
    original_query: str = Field(default="", description="사용자의 원본 질문")
    sql_query: str = Field(default="", description="생성된 SQL 쿼리")
    sql_result: List[Dict] = Field(default_factory=list, description="SQL 실행 결과")
    error: str = Field(default="", description="에러 메시지")

### 4. 핵심 도구 함수 정의

# DB 스키마 정보 생성 함수 정의

def get_db_schema_info() -> str | None:
    """데이터베이스 스키마 정보를 반환합니다."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        return "DATABASE_URL 환경변수가 설정되지 않았습니다."
    
    try:
        with psycopg2.connect(db_url) as conn:
            cursor = conn.cursor()
            # PostgreSQL 정보 스키마 조회
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'quarterly_sales';
            """)
            columns = cursor.fetchall()
            if not columns:
                return "테이블 정보를 찾을 수 없습니다."
            
            schema_str = "Table: quarterly_sales\nColumns:\n"
            for col_name, data_type in columns:
                schema_str += f"- {col_name}: {data_type}\n"
            return schema_str
    except Exception as e:
        return f"스키마 조회 중 오류 발생: {e}"

# # 기존 get_db_schema_info 함수를 아래 코드로 대체
# def get_db_schema_info() -> str | None:
#     """데이터베이스 스키마 정보를 반환합니다. (Hardcoded for stability)"""
    
#     # create_database_openapi.py와 동일한 스키마 정의
#     schema_str = """
#     Table: quarterly_sales
#     Columns:
#     - year_quarter: TEXT (예: '20241' = 2024년 1분기)
#     - district_type: TEXT (상권구분코드명)
#     - district_code: TEXT (상권코드)
#     - district_name: TEXT (상권명, 예: '강남역', '성수동카페거리')
#     - service_category_code: TEXT (서비스업종코드)
#     - service_category_name: TEXT (서비스업종명, 예: '한식음식점', '커피-음료')
#     - monthly_sales_amount: BIGINT (월평균 매출액)
#     - monthly_sales_count: BIGINT (월평균 매출건수)
#     - weekday_sales_amount: BIGINT (주중 매출액)
#     - weekend_sales_amount: BIGINT (주말 매출액)
#     - sales_time_11_14: BIGINT (점심시간 11~14시 매출)
#     - sales_time_17_21: BIGINT (저녁시간 17~21시 매출)
#     - male_sales_amount: BIGINT (남성 매출액)
#     - female_sales_amount: BIGINT (여성 매출액)
#     - sales_by_age_10s: BIGINT (10대 매출액)
#     - sales_by_age_20s: BIGINT (20대 매출액)
#     - sales_by_age_30s: BIGINT (30대 매출액)
#     - sales_by_age_40s: BIGINT (40대 매출액)
#     - sales_by_age_50s: BIGINT (50대 매출액)
#     - sales_by_age_60s_above: BIGINT (60대 이상 매출액)
#     """
#     return schema_str

def execute_sql_query(sql: str) -> List[Dict] | str:
    """SQL 쿼리를 실행하고 결과를 반환합니다."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        return "DATABASE_URL 환경변수가 설정되지 않았습니다."

    try:
        with psycopg2.connect(db_url) as conn:
            from psycopg2.extras import RealDictCursor
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        return f"SQL 실행 오류: {e}"

### 5. LangGraph 노드(Node) 정의

async def sql_generation_node(state: AnalysisState) -> Dict[str, Any]:
    """사용자 질문을 바탕으로 최적화된 SQL을 생성하는 노드"""
    print("\n[Node: SQL Generation]")
    user_query = state.messages[-1].content
    db_schema = get_db_schema_info()

    # [전문가 수정] WITH문(CTE)과 집계(Aggregation) 전략이 포함된 프롬프트
    prompt = f"""
    당신은 대한민국 서울시 상권분석을 위한 PostgreSQL 쿼리 작성 전문가입니다.
    아래 가이드라인을 엄격히 준수하여 사용자 질문에 가장 적합한 SQL을 작성하세요.

    ### 1. 데이터베이스 스키마 정보
    {db_schema}

    ### 2. [필수] SQL 작성 전략 가이드

    **상황 A: 시계열 비교 (성장률, 증가량 분석)**
    - 질문 예: "2024년 1분기 대비 2025년 1분기 30대 매출이 가장 많이 늘어난 상권은?"
    - **전략:** 반드시 **WITH문(CTE)**을 사용하여 각 시점의 데이터를 먼저 집계한 후 조인(Join)하십시오.
    - **주의:** `quarterly_sales` 테이블은 '상권+업종' 단위로 데이터가 있습니다. 특정 업종을 언급하지 않았다면 반드시 `district_name`으로 `GROUP BY`하여 합계를 구해야 합니다.
    
    *(올바른 작성 예시)*
    ```sql
    WITH q1 AS (
        -- 과거 시점 집계
        SELECT district_name, SUM(sales_by_age_30s) as sales_prev
        FROM quarterly_sales 
        WHERE year_quarter = '20241' 
        GROUP BY district_name
    ),
    q2 AS (
        -- 현재 시점 집계
        SELECT district_name, SUM(sales_by_age_30s) as sales_curr
        FROM quarterly_sales 
        WHERE year_quarter = '20251' 
        GROUP BY district_name
    )
    SELECT 
        q2.district_name,
        (q2.sales_curr - q1.sales_prev) as sales_increase
    FROM q2
    JOIN q1 ON q2.district_name = q1.district_name
    ORDER BY sales_increase DESC
    LIMIT 3;
    ```

    **상황 B: 단순 순위 및 조건 검색**
    - 질문 예: "2024년 전체 기간 동안 골목상권 중 매출 1위는?"
    - **전략:** `WHERE` 절로 조건을 걸고, `GROUP BY district_name` 후 `SUM`을 사용합니다.
    - 예: `WHERE district_type LIKE '%골목상권%' AND year_quarter LIKE '2024%'`

    ### 3. 출력 제약 사항
    - 오직 실행 가능한 순수 PostgreSQL 쿼리 텍스트만 반환하세요.
    - Markdown 코드 블록(```sql ... ```)이나 설명은 절대 포함하지 마세요.
    - 결과 행 수는 질문에 명시되지 않았다면 기본 `LIMIT 5`를 적용하세요.

    ### 사용자의 질문:
    {user_query}
    """
    
    response = await llm.ainvoke(prompt)
    # 마크다운 코드 블록 제거 및 공백 정리
    sql_query = response.content.strip().replace('```sql', '').replace('```', '').strip()
    
    print(f"-> 생성된 SQL:\n{sql_query}")
    return {"original_query": user_query, "sql_query": sql_query}

def sql_validation_node(state: AnalysisState) -> Dict[str, Any]:
    """생성된 SQL을 검증하는 노드"""
    print("\n[Node: SQL Validation]")
    sql_query = state.sql_query.upper()
    
    forbidden_keywords = ["DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT"]
    for keyword in forbidden_keywords:
        if keyword in sql_query:
            error_msg = f"보안 경고: 허용되지 않는 키워드({keyword})가 포함되어 있습니다."
            print(f"-> {error_msg}")
            return {"error": error_msg}
    
    return {"error": ""}

async def sql_execution_node(state: AnalysisState) -> Dict[str, Any]:
    """생성된 SQL을 실행하는 노드"""
    print("\n[Node: SQL Execution]")
    
    if state.error:
        print("-> 에러가 존재하여 실행을 건너뜀")
        return {"sql_result": []}

    sql_query = state.sql_query
    # 비동기 환경에서 동기 함수 실행을 위해 run_in_executor 사용 권장 (혹은 to_thread)
    result = await asyncio.to_thread(execute_sql_query, sql_query)
    
    if isinstance(result, str):
        # 실행 에러 발생 시
        return {"error": result, "sql_result": []}
        
    print(f"-> 실행 결과: {len(result)}개 행 조회")
    return {"sql_result": result}

async def report_generation_node(state: AnalysisState) -> Dict[str, Any]:
    """최종 보고서를 생성하는 노드 (SQL 해석 능력 강화)"""
    print("\n[Node: Report Generation]")
    
    if state.error:
        return {"messages": [AIMessage(content=f"요청을 처리하는 중 문제가 발생했습니다.\n이유: {state.error}")]}

    original_query = state.original_query
    sql_query = state.sql_query
    sql_result = state.sql_result

    if not sql_result:
        report = "분석 결과, 해당 조건에 맞는 데이터가 없습니다.\n조건을 변경하여 다시 질문해 주세요."
    else:
        # Decimal 등 JSON 직렬화 불가 객체 처리
        def default_converter(o):
            if isinstance(o, decimal.Decimal):
                return int(o)
            return str(o)

        json_result = json.dumps(sql_result, indent=2, ensure_ascii=False, default=default_converter)

        # [전문가 수정] SQL 쿼리를 프롬프트에 포함하여 데이터 문맥(Context) 이해도 향상
        prompt = f"""
        당신은 전문 데이터 분석가이자 보고서 작성가입니다.
        
        ### 분석 작업 정보
        1. **사용자 질문:** {original_query}
        2. **실행된 SQL 쿼리:** {sql_query}
        
        ### 데이터베이스 조회 결과:
        {json_result}

        ### 작성 가이드
        - 위 SQL 쿼리의 로직(예: 어떤 기간을 비교했는지, 어떤 컬럼을 뺐는지)을 이해하고 결과를 해석하세요.
        - 단순 나열이 아닌, "가장 높은 곳은 어디이며, 얼마나 증가했는지" 등 인사이트 위주로 서술하세요.
        - 금액 단위가 크다면 '억', '천만' 등으로 가독성 있게 표현하세요.
        - 보고서는 깔끔한 마크다운 형식으로 작성하세요.
        """
        response = await llm.ainvoke(prompt)
        report = response.content

    final_content = f"### 분석 보고서\n{report}\n\n---\n\n### 실행된 SQL 쿼리\n```sql\n{sql_query}\n```"
    return {"messages": [AIMessage(content=final_content)]}

### 6. 그래프 생성 함수
def create_agent():
    memory = MemorySaver()
    
    graph_builder = StateGraph(AnalysisState)
    graph_builder.add_node("generate_sql", sql_generation_node)
    graph_builder.add_node("validate_sql", sql_validation_node)
    graph_builder.add_node("execute_sql", sql_execution_node)
    graph_builder.add_node("generate_report", report_generation_node)
    
    graph_builder.set_entry_point("generate_sql")
    graph_builder.add_edge("generate_sql", "validate_sql")
    
    # 조건부 엣지: 에러 발생 시 리포트 생성으로 건너뜀
    def check_error(state: AnalysisState):
        if state.error:
            return "generate_report"
        return "execute_sql"

    graph_builder.add_conditional_edges(
        "validate_sql",
        check_error,
        {
            "generate_report": "generate_report",
            "execute_sql": "execute_sql"
        }
    )
    
    graph_builder.add_edge("execute_sql", "generate_report")
    graph_builder.add_edge("generate_report", END)

    return graph_builder.compile(checkpointer=memory)

### 7. 메인 실행
async def main():
    agent_executor = create_agent()
    
    print("==================================================")
    print("      서울시 상권 분석 전문 AI 에이전트      ")
    print("==================================================")
    
    thread_id = str(uuid.uuid4())
    
    while True:
        try:
            user_input = input("\n사용자: ")
            if user_input.lower() in ["exit", "종료"]:
                break

            config = {"configurable": {"thread_id": thread_id}}
            print("AI 에이전트: (분석 중...)")

            final_state = await agent_executor.ainvoke(
                {"messages": [HumanMessage(content=user_input)]}, 
                config=config
            )
            
            final_answer = final_state['messages'][-1].content
            print("\n" + "="*25 + " 최종 결과 " + "="*25)
            print(final_answer)
            print("="*62)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[CRITICAL] 오류 발생: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램 실행이 중단되었습니다.")
