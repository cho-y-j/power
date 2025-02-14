import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import streamlit as st
import os
from dotenv import load_dotenv
import string

load_dotenv()
DB_URL = os.getenv("DB_URL")

def get_db_connection():
    """데이터베이스 연결을 생성합니다."""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결 실패: {e}")
        return None

def create_table(cursor, table_name: str, columns: list, dtypes: list) -> bool:
    """지정된 컬럼과 데이터 타입을 사용해 테이블을 생성합니다. 기존 테이블은 삭제합니다."""
    try:
        column_definitions = []
        for col, dtype in zip(columns, dtypes):
            if dtype in ['int64', 'float64']:
                col_type = 'NUMERIC'
            elif dtype == 'datetime64[ns]':
                col_type = 'TIMESTAMP'
            else:
                col_type = 'TEXT'
            column_definitions.append(f"{col} {col_type}")
        
        create_table_query = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                {', '.join(column_definitions)}
            );
        """
        cursor.execute(create_table_query)
        return True
    except Exception as e:
        st.error(f"테이블 생성 실패: {e}")
        return False

def save_to_postgres(df: pd.DataFrame, table_name: str) -> bool:
    """데이터프레임의 데이터를 PostgreSQL에 저장합니다."""
    conn = None
    cursor = None
    try:
        df = df.copy().fillna('')  # 결측치 처리
        
        # 테이블명 정리 (소문자, 특수문자 제거)
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_').lower()
        
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        columns = list(df.columns)
        dtypes = [str(df[col].dtype) for col in columns]
        if not create_table(cursor, table_name, columns, dtypes):
            return False
        
        # 날짜/시간 컬럼 포맷 변경
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        values = [tuple(x) for x in df.values]
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s;
        """
        execute_values(cursor, insert_query, values, page_size=1000)
        
        conn.commit()
        st.success(f"✅ 데이터가 '{table_name}' 테이블에 성공적으로 저장되었습니다!")
        return True
        
    except Exception as e:
        st.error(f"데이터 저장 실패: {str(e)}")
        if cursor:
            cursor.execute("ROLLBACK")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_from_postgres(table_name: str) -> pd.DataFrame:
    """지정한 테이블의 데이터를 PostgreSQL에서 불러와 DataFrame으로 반환합니다."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        # 테이블명 검증 추가
        if not table_name.isalnum() and not all(c in (string.ascii_letters + string.digits + '_') for c in table_name):
            raise ValueError("유효하지 않은 테이블명입니다")
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"데이터 조회 실패: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def list_tables() -> list:
    """현재 public 스키마에 있는 테이블 목록을 반환합니다."""
    conn = get_db_connection()
    if conn is None:
        return []
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        df = pd.read_sql(query, conn)
        return df['table_name'].tolist()
    except Exception as e:
        st.error(f"테이블 목록 조회 실패: {e}")
        return []
    finally:
        conn.close()

# ---------- 분석 결과 아카이브 관련 함수 ----------

def create_analysis_archive_table():
    """분석 결과를 저장할 테이블(analysis_archive)이 없으면 생성합니다."""
    conn = get_db_connection()
    if conn is None:
        return
    try:
        cursor = conn.cursor()
        create_query = """
            CREATE TABLE IF NOT EXISTS analysis_archive (
                id SERIAL PRIMARY KEY,
                file_name TEXT,
                sheet_name TEXT,
                upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                table_structure TEXT,
                analysis_queries TEXT,
                insights TEXT,
                raw_sample_data TEXT,
                ppt_report TEXT
            );
        """
        cursor.execute(create_query)
        conn.commit()
    except Exception as e:
        st.error(f"분석 결과 아카이브 테이블 생성 실패: {e}")
    finally:
        if cursor:
            cursor.close()
        conn.close()

def save_analysis_result(file_name: str, sheet_name: str, table_structure: str, analysis_queries: str,
                         insights: str, raw_sample_data: str, ppt_report: str = None):
    """분석 결과를 analysis_archive 테이블에 저장합니다."""
    create_analysis_archive_table()
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO analysis_archive (file_name, sheet_name, table_structure, analysis_queries, insights, raw_sample_data, ppt_report)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (file_name, sheet_name, table_structure, analysis_queries, insights, raw_sample_data, ppt_report))
        conn.commit()
        st.success("✅ 분석 결과가 아카이브에 저장되었습니다!")
        return True
    except Exception as e:
        st.error(f"분석 결과 저장 실패: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        conn.close()

def list_analysis_results() -> pd.DataFrame:
    """analysis_archive 테이블에 저장된 분석 결과를 DataFrame으로 반환합니다."""
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        query = "SELECT id, file_name, sheet_name, upload_time FROM analysis_archive ORDER BY upload_time DESC;"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"분석 결과 조회 실패: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

