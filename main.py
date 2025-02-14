import streamlit as st
import pandas as pd
import tempfile, logging
import os
# 기존 모듈 import
import ai_service
import database as db
import report_generator as rpt

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Streamlit Secrets에서 환경 변수 로드
try:
    OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
    DB_URL = st.secrets["DB_URL"]
except KeyError as e:
    st.error(f"🚨 환경 변수 {e}가 설정되지 않았습니다! Streamlit Cloud 'Secrets'에서 설정해주세요.")
    st.stop()

# 환경 변수 확인 로그
logging.info(f"✅ OpenAI API Key 및 DB URL 로드 완료")

# 필수 환경 변수 확인
REQUIRED_VARS = ["OPENAI_API_KEY", "DB_URL"]
missing = [var for var in REQUIRED_VARS if var not in st.secrets]
if missing:
    st.error(f"🚨 필수 환경 변수가 설정되지 않았습니다: {', '.join(missing)}")
    st.stop()

# 캐시된 DataManager (추후 확장을 위해 클래스로 감쌀 수 있음)
# 캐시된 DataManager (추후 확장을 위해 클래스로 감쌀 수 있음)
class DataManager:
    @staticmethod
    def save_dataframe(df: pd.DataFrame, table_name: str) -> bool:
        return db.save_to_postgres(df, table_name)

    @staticmethod
    def fetch_dataframe(table_name: str) -> pd.DataFrame:
        return db.fetch_from_postgres(table_name)
    
    @staticmethod
    def list_tables() -> list:
        return db.list_tables()
    
    @staticmethod
    def save_analysis(file_name: str, sheet_name: str, table_structure: str, analysis_queries: str,
                      insights: str, raw_sample_data: str, ppt_report: str = None) -> bool:
        return db.save_analysis_result(file_name, sheet_name, table_structure, analysis_queries, insights, raw_sample_data, ppt_report)
    
    @staticmethod
    def list_analysis() -> pd.DataFrame:
        return db.list_analysis_results()

# 임시 파일 정리 함수
def cleanup_temp_files(*paths):
    for path in paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                logging.error(f"임시 파일 삭제 오류: {e}")

# 페이지별 함수 정의

def upload_and_analyze_page():
    st.title("📂 데이터 업로드 및 AI 분석")
    uploaded_file = st.file_uploader("파일을 업로드하세요 (엑셀/CSV)", type=["xlsx", "csv"])
    save_archive = st.checkbox("분석 결과 자동 아카이브 저장", value=True)
    generate_visuals = st.checkbox("시각화 및 PPT 보고서 생성", value=False)

    if uploaded_file:
        tmp_path = None
        chart_path = None
        ppt_filename = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
                tmp.write(uploaded_file.getbuffer())
                tmp_path = tmp.name

            # 파일 읽기 (엑셀 또는 CSV)
            if uploaded_file.name.endswith("xlsx"):
                sheets = pd.read_excel(tmp_path, sheet_name=None)
                if not sheets:
                    st.error("엑셀 파일에 시트가 없습니다.")
                    return
                sheet_names = list(sheets.keys())
                selected_sheet = st.selectbox("분석할 시트를 선택하세요", sheet_names)
                df = sheets[selected_sheet]
            else:
                df = pd.read_csv(tmp_path)

            st.subheader("데이터 미리보기")
            st.dataframe(df.head())

            # 컬럼명 정리 (기존 ai_service.clean_column_names 사용) :contentReference[oaicite:6]{index=6}
            df.columns = ai_service.clean_column_names(df.columns)

            # DB 저장
            table_name = os.path.splitext(uploaded_file.name)[0].replace(" ", "_").lower()
            if DataManager.save_dataframe(df, table_name):
                st.success("✅ 데이터베이스에 저장되었습니다.")
            else:
                st.error("🚨 데이터 저장 실패")
                return

            # AI 분석: 테이블 구조, 분석 쿼리, 경영 인사이트
            columns_info = df.dtypes.to_string()
            table_structure = ai_service.analyze_data_structure(columns_info)
            sample_data = df.head().to_csv(index=False)
            analysis_queries = ai_service.generate_analysis_queries(sample_data)
            insights = ai_service.generate_management_insights(sample_data)

            st.subheader("AI가 생성한 PostgreSQL 코드")
            st.code(table_structure, language="sql")
            st.subheader("AI가 생성한 분석 쿼리")
            st.code(analysis_queries, language="sql")
            st.subheader("경영 인사이트")
            st.write(insights)

            # 결과 아카이브 저장
            if save_archive:
                DataManager.save_analysis(uploaded_file.name, selected_sheet if uploaded_file.name.endswith("xlsx") else table_name,
                                            table_structure, analysis_queries, insights, sample_data)

            # 추가 시각화 및 PPT 생성
            if generate_visuals:
                # 간단한 차트 생성 (수치형 컬럼 우선 선택)
                numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                if numeric_cols:
                    numeric_col = numeric_cols[0]
                else:
                    numeric_col = df.columns[0]
                y_vals = df[numeric_col].dropna().astype(float).head(10).tolist()
                n = len(y_vals)
                chart_data = {"x": list(range(1, n + 1)), "y": y_vals}
                chart_path = rpt.generate_sample_chart(chart_data, title="샘플 시계열 분석")
                if chart_path:
                    st.image(chart_path, caption="분석 차트", use_column_width=True)
                    ppt_filename = rpt.create_ppt_report(insights, table_structure, chart_path)
                    if ppt_filename:
                        st.success(f"PPT 보고서가 생성되었습니다: {ppt_filename}")
                        with open(ppt_filename, "rb") as f:
                            st.download_button("PPT 보고서 다운로드", f, file_name=ppt_filename,
                                               mime="application/vnd.openxmlformats-officedocument.presentationml.presentation")
                    else:
                        st.error("PPT 보고서 생성 실패")
            else:
                st.info("시각화 및 PPT 보고서 생성을 원하시면 해당 옵션을 체크하세요.")
        except Exception as e:
            st.error(f"처리 중 오류 발생: {e}")
            logging.error(e)
        finally:
            cleanup_temp_files(tmp_path, chart_path, ppt_filename)

def dashboard_page():
    st.title("📊 데이터 대시보드")
    tables = DataManager.list_tables()
    if not tables:
        st.info("저장된 테이블이 없습니다.")
        return

    selected_table = st.selectbox("테이블 선택", tables)
    if st.button("데이터 조회"):
        df_stored = DataManager.fetch_dataframe(selected_table)
        if df_stored.empty:
            st.info("조회된 데이터가 없습니다.")
        else:
            st.subheader(f"'{selected_table}' 테이블 데이터")
            st.dataframe(df_stored)
            st.download_button("CSV 다운로드", df_stored.to_csv(index=False).encode('utf-8'),
                               file_name=f"{selected_table}.csv", mime="text/csv")
            st.info("PowerBI는 PostgreSQL DB 혹은 CSV 파일을 통해 데이터를 불러올 수 있습니다.")

def analysis_archive_page():
    st.title("🗂 분석 결과 아카이브")
    df_archive = DataManager.list_analysis()
    if df_archive.empty:
        st.info("저장된 분석 결과가 없습니다.")
    else:
        st.dataframe(df_archive)
        st.info("ID를 선택하여 상세 분석 결과를 확인하거나 다운로드 기능을 추가할 수 있습니다.")

def about_page():
    st.title("About")
    st.markdown("""
    ###  AI 경영/조직 진단 컨설팅 시스템  
    - **데이터 전처리 및 DB 저장**: 업로드된 파일을 PostgreSQL에 안전하게 저장합니다.  
    - **AI 분석 엔진**: OpenAI GPT-4를 기반으로 한 AI가 데이터 구조, 분석 쿼리 및 경영 인사이트를 도출합니다.  
    - **고급 시각화 및 보고서**: Matplotlib와 python-pptx를 활용해 고퀄리티 보고서를 생성합니다.  
    - **PowerBI 연동**: PostgreSQL DB 및 CSV 내보내기를 통해 PowerBI와 손쉽게 연동할 수 있습니다.
    
     
    ※ 이 시스템은 Docker, CI/CD, 로깅 등 운영환경에 맞춰 확장 가능합니다.
    """)

# 사이드바 네비게이션
page_options = {
    "데이터 업로드 & 분석": upload_and_analyze_page,
    "대시보드": dashboard_page,
    "분석 결과 아카이브": analysis_archive_page,
    "About": about_page
}

st.sidebar.title("메뉴")
selection = st.sidebar.radio("페이지 선택", list(page_options.keys()))
page_options[selection]()
