import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import asyncio
import os
import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
# data_analysis_langgraph 파일이 같은 경로에 있어야 합니다.
from data_analysis_langgraph import create_agent 

# 1. 환경 설정 및 세션 상태 초기화
st.set_page_config(page_title="서울시 상권 분석 BI", layout="wide")

# .env 로드 (로컬 개발용)
try:
    load_dotenv()
except Exception:
    pass

# 2. 인증 (Authentication) 설정
# v0.2.3 버전에 최적화된 설정입니다.
config = {
    'credentials': {
        'usernames': {
            'admin': {
                'name': 'Admin User',
                'email': 'admin@example.com',
                # 초기값은 임시로 둡니다. 아래에서 덮어씌웁니다.
                'password': 'placeholder_will_be_replaced' 
            }
        }
    },
    'cookie': {
        'expiry_days': 30,
        'key': 'some_signature_key',
        'name': 'some_cookie_name',
    },
    'preauthorized': {
        'emails': []
    }
}

# [핵심 수정] 1234에 대한 해시값 생성 (v0.2.3 호환)
# 복잡한 try-except 없이 명확하게 호출합니다.
from streamlit_authenticator import Hasher
hashed_passwords = Hasher(['1234']).generate()

# 생성된 해시값을 config에 주입
config['credentials']['usernames']['admin']['password'] = hashed_passwords[0]

# 인증 객체 생성
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

# 로그인 위젯
# v0.2.3에서는 login()이 (name, status, username) 튜플을 반환합니다.
name, authentication_status, username = authenticator.login('main')

if authentication_status is False:
    st.error('Username/password is incorrect')
    st.stop()
elif authentication_status is None:
    st.warning('Please enter your username and password')
    st.stop()

# 로그인 성공 시
st.sidebar.write(f'Welcome *{name}*')
authenticator.logout('Logout', 'sidebar')

# 3. LangGraph 에이전트 초기화 (세션 상태에 저장)
if "agent" not in st.session_state:
    st.session_state.agent = create_agent()
if "messages" not in st.session_state:
    st.session_state.messages = []
if "thread_id" not in st.session_state:
    import uuid
    st.session_state.thread_id = str(uuid.uuid4())

# 4. Async Helper Function
# 비동기 충돌 방지를 위한 안전한 래퍼 함수
def run_async(coroutine):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    
    if loop and loop.is_running():
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coroutine)
    else:
        return asyncio.run(coroutine)

# 5. UI 구성
st.title("📊 서울시 상권 분석 AI 비서")
st.markdown("서울시 상권 데이터를 기반으로 질문에 답변하고 시각화를 제공합니다.")

# 기존 대화 내용 표시
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# 사용자 입력 처리
if prompt := st.chat_input("질문을 입력하세요 (예: 2024년 1분기 강남구 매출 보여줘)"):
    # 사용자 메시지 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # AI 답변 생성
    with st.chat_message("assistant"):
        with st.spinner("분석 중입니다..."):
            try:
                # LangGraph 실행 설정
                graph_config = {"configurable": {"thread_id": st.session_state.thread_id}}
                
                # 비동기 실행
                final_state = run_async(
                    st.session_state.agent.ainvoke(
                        {"messages": [HumanMessage(content=prompt)]},
                        config=graph_config
                    )
                )
                
                # 결과 파싱 및 출력
                response_content = final_state['messages'][-1].content
                st.markdown(response_content)
                
                # 데이터 시각화 처리
                if 'sql_result' in final_state and final_state['sql_result']:
                    data = final_state['sql_result']
                    df = pd.DataFrame(data)
                    
                    if not df.empty:
                        st.divider()
                        st.subheader("📈 데이터 시각화")
                        
                        with st.expander("데이터 원본 보기"):
                            st.dataframe(df)

                        # 자동 차트 생성 로직
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        obj_cols = df.select_dtypes(include=['object']).columns.tolist()
                        
                        if numeric_cols:
                            x_axis = obj_cols[0] if obj_cols else None
                            if x_axis:
                                # 인덱스 설정 후 상위 10개만 시각화 (가독성 위해)
                                chart_data = df.set_index(x_axis)[numeric_cols[:3]].head(10)
                                st.bar_chart(chart_data)
                            else:
                                st.bar_chart(df[numeric_cols[:3]])
                
                # 대화 기록 저장
                st.session_state.messages.append({"role": "assistant", "content": response_content})
                
            except Exception as e:
                st.error(f"오류가 발생했습니다: {e}")
