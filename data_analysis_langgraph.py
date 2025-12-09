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

# SQL 쿼리 실행 함수 정의
def execute_sql_query(sql: str) -> List[Dict] | str:
    """SQL 쿼리를 실행하고 결과를 반환합니다."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        return "DATABASE_URL 환경변수가 설정되지 않았습니다."

    try:
        with psycopg2.connect(db_url) as conn:
            # 딕셔너리 형태로 결과를 받기 위해 RealDictCursor 사용
            from psycopg2.extras import RealDictCursor
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(sql)
            return [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        return f"SQL 실행 오류: {e}"

### 5. LangGraph 노드(Node) 정의

async def sql_generation_node(state: AnalysisState) -> Dict[str, Any]:
    """사용자 질문을 바탕으로 SQL을 생성하는 노드"""
    print("\n[Node: SQL Generation]")
    user_query = state.messages[-1].content
    db_schema = get_db_schema_info()

    prompt = f"""
    당신은 대한민국 서울시 상권분석 전문가이자 PostgreSQL 마스터입니다.
    아래 DB 스키마와 컬럼 의미를 참고하여, 사용자 질문에 가장 적합한 PostgreSQL 쿼리를 생성해주세요.

    ### 데이터베이스 스키마:
    {db_schema}
    
    ### 주요 컬럼 의미 (영문 컬럼명 -> 한글 의미):
    - year_quarter: 기준년도분기 (예: '20241' = 2024년 1분기)
    - district_name: 상권명
    - service_category_name: 서비스 업종명
    - monthly_sales_amount: 월평균 추정 매출액
    - monthly_sales_count: 월평균 추정 매출 건수
    - midweek_sales_amount: 주중 매출액
    - weekend_sales_amount: 주말 매출액
    - sales_time_11_14: 점심시간(11시~14시) 매출액
    - sales_time_17_21: 저녁시간(17시~21시) 매출액
    - male_sales_amount: 남성 매출액
    - female_sales_amount: 여성 매출액
    - sales_by_age_30s: 30대 연령층의 매출액
    - 예를 들어, 사용자가 '점심 시간'을 언급하면 `sales_time_11_14` 컬럼을 사용해야 합니다.

    ### 사용자의 질문:
    {user_query}

    - 다른 설명 없이 오직 실행 가능한 PostgreSQL 쿼리만 생성해주세요.
    - 마크다운 코드 블록(```sql ... ```)은 사용하지 마세요. 순수 SQL 텍스트만 반환하세요.
    """
    response = await llm.ainvoke(prompt)
    sql_query = response.content.strip().replace('`', '').replace('sql', '')
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
        print("-> 에러가 존재하여 실행을 건너뜠습니다.")
        return {"sql_result": []}

    sql_query = state.sql_query
    result = await asyncio.to_thread(execute_sql_query, sql_query)
    
    if isinstance(result, str):
        # 실행 에러 발생 시
        return {"error": result, "sql_result": []}
        
    print(f"-> 실행 결과: {len(result)}개 행 조회")
    return {"sql_result": result}

# async def report_generation_node(state: AnalysisState) -> Dict[str, Any]:
#     """최종 보고서를 생성하고 상태를 업데이트하는 노드"""
#     print("\n[Node: Report Generation]")
    
#     if state.error:
#         return {"messages": [AIMessage(content=f"요청을 처리하는 중 문제가 발생했습니다.\n이유: {state.error}")]}

#     original_query = state.original_query
#     sql_query = state.sql_query
#     sql_result = state.sql_result

#     if not sql_result:
#         report = "분석 결과, 해당 조건에 맞는 데이터가 없습니다."
#     else:
#         prompt = f"""
#         당신은 전문 데이터 분석가이자 보고서 작성가입니다.
#         다음은 사용자의 원본 질문과 데이터베이스에서 추출한 분석 결과입니다.
#         이 데이터를 단순히 나열하지 말고, 사용자가 질문한 의도에 맞춰 의미 있는 인사이트를 도출하고, 비교 및 분석하여 상세한 최종 보고서를 마크다운 형식으로 작성해주세요.

#         ### 원본 사용자 질문:
#         {original_query}

#         ### 데이터베이스 조회 결과 (JSON 형식):
#         {json.dumps(sql_result, indent=2, ensure_ascii=False)}

#         ### 최종 분석 보고서 (마크다운 형식):
#         """
#         response = await llm.ainvoke(prompt)
#         report = response.content

#     final_content = f"### 분석 보고서\n{report}\n\n---\n\n### 실행된 SQL 쿼리\n```sql\n{sql_query}\n```"
#     return {"messages": [AIMessage(content=final_content)]}

async def report_generation_node(state: AnalysisState) -> Dict[str, Any]:
    """
    최종 보고서를 생성하고 상태를 업데이트하는 노드,
    JSON 변환 로직이 강화
    """
    print("\n[Node: Report Generation]")
    
    if state.error:
        return {"messages": [AIMessage(content=f"요청을 처리하는 중 문제가 발생했습니다.\n이유: {state.error}")]}

    original_query = state.original_query
    sql_query = state.sql_query
    sql_result = state.sql_result

    if not sql_result:
        report = "분석 결과, 해당 조건에 맞는 데이터가 없습니다."
    else:
        # [핵심 수정] Decimal 타입을 처리하기 위한 커스텀 인코더 함수
        def decimal_default(obj):
            if isinstance(obj, decimal.Decimal):
                return int(obj)  # Decimal을 int로 변환 (금액 등은 정수가 보기에 좋음)
            return str(obj)      # 그 외 알 수 없는 타입은 문자열로 변환

        # json.dumps에 default=decimal_default 추가
        json_result = json.dumps(sql_result, indent=2, ensure_ascii=False, default=decimal_default)

        prompt = f"""
        당신은 전문 데이터 분석가이자 보고서 작성가입니다.
        다음은 사용자의 원본 질문과 데이터베이스에서 추출한 분석 결과입니다.
        이 데이터를 단순히 나열하지 말고, 사용자가 질문한 의도에 맞춰 의미 있는 인사이트를 도출하고, 비교 및 분석하여 상세한 최종 보고서를 마크다운 형식으로 작성해주세요.

        ### 원본 사용자 질문:
        {original_query}

        ### 데이터베이스 조회 결과 (JSON 형식):
        {json_result}

        ### 최종 분석 보고서 (마크다운 형식):
        """
        response = await llm.ainvoke(prompt)
        report = response.content

    final_content = f"### 분석 보고서\n{report}\n\n---\n\n### 실행된 SQL 쿼리\n```sql\n{sql_query}\n```"
    return {"messages": [AIMessage(content=final_content)]}

### 6. 그래프 생성 함수 (외부 호출용)
def create_agent():
    # 메모리 저장소 (In-Memory)
    memory = MemorySaver()
    
    # 그래프 구성
    graph_builder = StateGraph(AnalysisState)
    graph_builder.add_node("generate_sql", sql_generation_node)
    graph_builder.add_node("validate_sql", sql_validation_node)
    graph_builder.add_node("execute_sql", sql_execution_node)
    graph_builder.add_node("generate_report", report_generation_node)
    
    graph_builder.set_entry_point("generate_sql")
    graph_builder.add_edge("generate_sql", "validate_sql")
    
    # 조건부 엣지 대신 노드 내부에서 에러 체크 후 분기 처리하는 방식 사용 (여기서는 단순 선형 연결하되, 노드 내부에서 state.error 체크)
    # 더 명시적인 방법: add_conditional_edges 사용
    def check_error(state: AnalysisState):
        if state.error:
            return "generate_report" # 에러가 있으면 바로 리포트로 (리포트에서 에러 출력)
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

### 7. 콘솔 실행 로직 (테스트용)
async def main():
    agent_executor = create_agent()
    
    print("==================================================")
    print("      서울시 상권 분석 전문 AI 에이전트 (콘솔 모드)      ")
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
