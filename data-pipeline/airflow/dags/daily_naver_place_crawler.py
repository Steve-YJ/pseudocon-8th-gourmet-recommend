from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils import get_keyword, run_crawler  # utils.py에서 함수 임포트

default_args = {
    'owner': 'youngjeon',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 10),
    'email': ['pseudocon.24.summer@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'daily_naver_place_crawler',
    default_args=default_args,
    description='Naver Place Crawler DAG',
    schedule_interval="*/1 * * * *",
    catchup=False,
    doc_md="""
    ### 네이버 플레이스 크롤러 DAG
    
    이 DAG는 네이버 플레이스에서 음식점 데이터를 매일 자동으로 크롤링하는 과정을 자동화하도록 설계되었습니다. DAG는 두 가지 주요 작업으로 구성됩니다:
    
    1. **get_keyword**:
       - Redis 큐에서 검색 키워드를 가져옵니다.
       - 이 키워드는 네이버 플레이스에서 음식점 데이터를 조회하는 데 사용됩니다.
    
    2. **run_crawler**:
       - 가져온 키워드를 사용하여 네이버 플레이스에서 검색 쿼리를 수행합니다.
       - 가져온 데이터를 처리하여 Google Cloud Storage(GCS)에 업로드합니다.
       - 이 작업은 재시도 로직을 포함하고 있으며, 비율 제한을 유연하게 처리하여 안정적인 데이터 수집을 보장합니다.
    
    DAG는 매일 실행되며 기본적인 오류 처리 및 재시도 메커니즘을 포함합니다.
    
    #### 작업 세부 사항:
    
    - `get_keyword`:
      - Redis 큐에서 키워드를 가져옵니다.
      - 가져온 키워드를 XCom에 저장하여 후속 작업에서 사용할 수 있도록 합니다.
    
    - `run_crawler`:
      - XCom에서 키워드를 가져옵니다.
      - 네이버 플레이스 API에 요청을 보내 음식점 데이터를 가져옵니다.
      - 가져온 데이터를 Parquet 형식으로 GCS에 업로드합니다.
      - 모든 사용 가능한 데이터를 수집하기 위해 페이지네이션을 관리합니다.
    
    ### 구성:
    
    - **소유자**: youngjeon
    - **시작 날짜**: 2024년 6월 10일
    - **오류 발생 시 이메일**: 네
    - **재시도 횟수**: 1
    - **재시도 지연 시간**: 5분
    - **스케줄 간격**: 매 5분 간격
    
    ### 사용법:
    
    이 DAG는 네이버 플레이스에서 음식점 데이터를 매일 수집하는 작업을 자동화하는 데 적합합니다. 수집된 데이터는 데이터 분석, 보고서 작성, 비즈니스 인텔리전스와 같은 다양한 목적으로 사용될 수 있습니다.
    
    """
)

get_keyword_task = PythonOperator(
    task_id='get_keyword',
    python_callable=get_keyword,
    provide_context=True,
    dag=dag,
)

run_crawler_task = PythonOperator(
    task_id='run_crawler',
    python_callable=run_crawler,
    provide_context=True,
    dag=dag
)

get_keyword_task >> run_crawler_task
