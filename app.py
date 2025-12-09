import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import asyncio
import os
import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from data_analysis_langgraph import create_agent

# 1. 환경 설정 및 세션 상태 초기화
st.set_page_config(page_title="서울시 상권 분석 BI", layout="wide")

# .env 로드 (로컬 개발용)
try:
    load_dotenv()
except Exception:
    pass

# 2. 인증 (Authentication) 설정
# 실제 운영 환경에서는 비밀번호를 환경변수나 보안 저장소에서 관리해야 합니다.
# 여기서는 예시를 위해 하드코딩된 딕셔너리를 사용합니다.
try:
    # 최신 버전 호환성을 위해 Hasher 사용
    from streamlit_authenticator.utilities.hasher import Hasher
except ImportError:
    # 구 버전 호환성
    from streamlit_authenticator import Hasher

passwords_to_hash = ['1234']
hashed_passwords = Hasher(passwords_to_hash).generate()

config = {
    'credentials': {
        'usernames': {
            'admin': {
                'name': 'Admin User',
                'password': hashed_passwords[0],
                'email': 'admin@example.com',
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

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# 로그인 위젯
authenticator.login('main')

if st.session_state["authentication_status"] is False:
    st.error('Username/password is incorrect')
    st.stop()
elif st.session_state["authentication_status"] is None:
    st.warning('Please enter your username and password')
    st.stop()

# 로그인 성공 시
st.sidebar.write(f'Welcome *{st.session_state["name"]}*')
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
# Streamlit은 기본적으로 동기 방식이므로, 비동기 함수를 실행하기 위한 래퍼가 필요합니다.
def run_async(coroutine):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    
    if loop and loop.is_running():
        # 이미 이벤트 루프가 실행 중인 경우 (Streamlit의 일부 환경)
        # nest_asyncio가 필요할 수 있으나, 여기서는 asyncio.run을 피하고
        # loop.create_task 등을 사용할 수 없으므로, 
        # 새로운 루프를 생성하거나 기존 루프를 활용하는 방식이 제한적임.
        # 가장 안전한 방법은 nest_asyncio를 사용하는 것이지만, 
        # 의존성을 줄이기 위해 간단한 트릭을 사용하거나, 
        # Streamlit이 허용하는 경우 asyncio.run()을 호출.
        # 하지만 "This event loop is already running" 에러를 피하기 위해:
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coroutine)
    else:
        return asyncio.run(coroutine)

# 5. UI 구성
st.title("📊 서울시 상권 분석 AI 비서")
st.markdown("서울시 상권 데이터를 기반으로 질문에 답변하고 시각화를 제공합니다.")

# 채팅 인터페이스
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        # 만약 메시지에 시각화 데이터가 포함되어 있다면 여기서 렌더링 (복잡도 증가로 생략, 답변 생성 시 처리)

if prompt := st.chat_input("질문을 입력하세요 (예: 2024년 1분기 강남구 매출 보여줘)"):
    # 사용자 메시지 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # AI 답변 생성
    with st.chat_message("assistant"):
        with st.spinner("분석 중입니다..."):
            try:
                # LangGraph 실행
                config = {"configurable": {"thread_id": st.session_state.thread_id}}
                
                # 비동기 실행을 위한 래퍼 호출
                final_state = run_async(
                    st.session_state.agent.ainvoke(
                        {"messages": [HumanMessage(content=prompt)]},
                        config=config
                    )
                )
                
                # 결과 파싱
                response_content = final_state['messages'][-1].content
                st.markdown(response_content)
                
                # 시각화 처리
                # state에 sql_result가 있고 데이터가 존재하면 시각화 시도
                if 'sql_result' in final_state and final_state['sql_result']:
                    data = final_state['sql_result']
                    df = pd.DataFrame(data)
                    
                    if not df.empty:
                        st.divider()
                        st.subheader("📈 데이터 시각화")
                        
                        # 데이터프레임 표시
                        with st.expander("데이터 원본 보기"):
                            st.dataframe(df)

                        # 간단한 시각화 추천 로직
                        # 숫자형 컬럼 찾기
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        # 문자열 컬럼 찾기 (X축 후보)
                        obj_cols = df.select_dtypes(include=['object']).columns.tolist()
                        
                        if numeric_cols:
                            # 기본적으로 첫 번째 문자열 컬럼을 X축, 첫 번째 숫자 컬럼을 Y축으로 설정
                            x_axis = obj_cols[0] if obj_cols else None
                            y_axis = numeric_cols[0]
                            
                            if x_axis:
                                st.bar_chart(df.set_index(x_axis)[numeric_cols[:3]]) # 최대 3개 지표 비교
                            else:
                                st.bar_chart(df[numeric_cols[:3]])
                                
                        # 지도 시각화 (위도/경도 컬럼이 있다면)
                        # 현재 스키마에는 위도/경도가 없으므로 생략하지만, 
                        # 만약 district_code로 매핑된 좌표가 있다면 st.map(df) 사용 가능
                
                # 세션에 저장
                st.session_state.messages.append({"role": "assistant", "content": response_content})
                
            except Exception as e:
                st.error(f"오류가 발생했습니다: {e}")
