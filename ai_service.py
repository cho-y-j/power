from openai import OpenAI
import os
from dotenv import load_dotenv

# Streamlit Secrets에서 환경 변수 가져오기
try:
    OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
except KeyError:
    st.error("🚨 OpenAI API 키가 설정되지 않았습니다! Streamlit Cloud 'Secrets'에서 설정해주세요.")
    st.stop()

# OpenAI 클라이언트 초기화
client = OpenAI(api_key=OPENAI_API_KEY)

def analyze_data_structure(df_info: str) -> str:
    """데이터 컬럼 정보를 기반으로 최적의 PostgreSQL 스키마를 생성하는 SQL 코드를 도출합니다."""
    if not df_info:
        return "Error: 데이터 정보가 없습니다."
    prompt = f"""
데이터 컬럼 정보를 분석하여 최적화된 PostgreSQL 테이블 구조를 설계해주세요:
{df_info}

요구사항:
1. 컬럼명에서 특수문자 제거 및 스네이크 케이스로 변환
2. 데이터 타입 최적화 (TEXT, INTEGER, DECIMAL, TIMESTAMP 등)
3. 인덱싱이 필요한 컬럼 식별
4. 외래 키 관계 추천 (필요한 경우)

응답 형식:
- 정리된 CREATE TABLE 문
- 인덱스 생성 문
- 데이터 타입 변환 필요성 분석
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "당신은 데이터 모델링 전문가입니다."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"OpenAI API 오류: {str(e)}"

def generate_analysis_queries(df_sample: str) -> str:
    """데이터 샘플을 기반으로 상세 분석 쿼리와 시각화 방법에 대한 인사이트를 도출합니다."""
    prompt = f"""
다음 데이터 샘플을 분석하여 상세한 인사이트를 도출할 수 있는 SQL 쿼리와 시각화 방법을 작성해주세요:
{df_sample}

관점:
1. 시계열 분석 (선 그래프, 히트맵 등)
2. 그룹별 분석 (박스플롯, 레이더 차트, 파이 차트 등)
3. 상관관계 분석 (히트맵, 산점도 매트릭스, 회귀분석)
4. 이상치 탐지 및 KPI 대시보드 (게이지, 불릿 차트 등)

응답 형식:
- 각 분석의 목적 및 설명
- 최적화된 SQL 쿼리
- 권장 시각화 방법 및 예상 비즈니스 인사이트
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "당신은 데이터 분석 및 시각화 전문가입니다. 항상 실용적이고 구체적인 인사이트를 제공합니다."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"OpenAI API error: {e}"

def generate_postgres_code(columns_info: str) -> str:
    """데이터 컬럼 정보를 토대로 최적의 PostgreSQL 테이블 생성 코드를 도출합니다."""
    prompt = f"""
다음은 데이터 컬럼 정보입니다:
{columns_info}

요구사항:
1. 시계열 데이터 분석을 위한 타임스탬프 컬럼 최적화
2. 숫자형 데이터는 INTEGER 또는 DECIMAL 등으로 최적 변환
3. Power BI 대시보드에 적합한 테이블 구조 설계
4. KPI 계산을 위한 집계 컬럼 추가

PostgreSQL 테이블 생성 코드를 작성해주세요.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "system", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"OpenAI API error: {e}"

def generate_management_insights(sample_data_csv: str) -> str:
    """경영평가 및 조직진단을 위한 주요 KPI, 인사이트 및 개선 제안을 도출합니다."""
    if not isinstance(sample_data_csv, str) or len(sample_data_csv.strip()) < 10:
        return "Error: 유효하지 않은 데이터 샘플입니다."
    prompt = f"""
아래 경영평가 및 조직진단을 위한 샘플 데이터를 바탕으로 주요 KPI, 인사이트 및 실행 가능한 개선 제안을 상세히 도출해주세요:
{sample_data_csv}

응답 형식:
- 주요 발견사항 및 비즈니스 인사이트
- 실행 가능한 개선 제안
- 권장 시각화 방법
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "system", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"OpenAI API error: {e}"

def clean_column_names(columns: list) -> list:
    """컬럼명을 정리하여 PostgreSQL에서 안전하게 사용될 수 있도록 합니다."""
    cleaned = []
    for col in columns:
        if not isinstance(col, str):
            col = f"column_{len(cleaned)}"
        clean_name = str(col).lower().strip()
        clean_name = clean_name.replace('unnamed: ', 'column_')
        clean_name = clean_name.replace(' ', '_').replace(':', '_').replace('-', '_')
        clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
        base_name = clean_name
        counter = 1
        while clean_name in cleaned:
            clean_name = f"{base_name}_{counter}"
            counter += 1
        cleaned.append(clean_name)
    return cleaned
