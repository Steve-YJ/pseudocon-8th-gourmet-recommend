import pytz
import time
import re
import requests
from datetime import timedelta, datetime
import pandas as pd
from korean_romanizer.romanizer import Romanizer

from google.cloud import storage
from google.oauth2 import service_account
from io import BytesIO

import redis
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# 한국어 검색 결과를 로마자로 변환해주는 함수
def korean_romanizer_converter(e):
    r = Romanizer(e)
    result = r.romanize()
    result = re.sub("yeok$", "_station", result)
    result = re.sub("dong$", "_dong", result)
    return result

# API 요청을 재시도 로직을 포함하여 수행하는 함수
def make_request_with_retry(url, headers, json_data, retries=5, backoff_factor=1.5):
    for i in range(retries):
        response = requests.post(url, headers=headers, json=json_data)
        if response.status_code == 429:  # 요청 제한에 걸린 경우
            sleep_time = backoff_factor * (2 ** i)
            print(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
        elif response.ok:
            return response  # 성공적인 응답 반환
        else:
            print(f"Request failed with status code: {response.status_code}")
        time.sleep(10)  # 10초 간격으로 요청 재시도
    return None

# 데이터 프레임을 Google Cloud Storage에 업로드하는 함수
def upload_to_gcs(data, bucket_name, destination_file_name, key_path):
    KST = pytz.timezone('Asia/Seoul')
    today = datetime.now(KST).date()
    date_path = today.strftime('%Y/%m/%d')

    credentials = service_account.Credentials.from_service_account_file(key_path)
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_file_name)

    df = pd.DataFrame(data)
    df.drop_duplicates(subset=['name'], inplace=True)  # 'name' 칼럼에 대해서 중복 제거

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    try:
        blob.upload_from_string(buffer.read(), content_type='application/octet-stream')
        print("파일이 성공적으로 업로드 되었습니다")
        print(f"파일명: {destination_file_name}")
        print("-" * 60)
    except Exception as ex:
        print(f"업로드 에러 발생: {ex}")

# Redis에서 키워드를 가져오는 함수
def get_keyword(**context):
    r = redis.Redis(
        host=Variable.get("redis_host"),
        port=Variable.get("redis_port"),
        password=Variable.get("redis_password"),
        decode_responses=True
    )
    search_keyword = r.rpop("search_term")  # 'search_term' 리스트의 마지막 값을 가져옴
    context['task_instance'].xcom_push(key='search_keyword', value=search_keyword)  # XCom에 값 저장
    return {'search_keyword': search_keyword}

# 크롤러 실행 함수
def run_crawler(**context):
    url = "https://pcmap-api.place.naver.com/place/graphql"
    headers = {
        "referer": "https://pcmap.place.naver.com/restaurant/list",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    search_keyword = context['task_instance'].xcom_pull(task_ids='get_keyword', key='search_keyword')  # XCom에서 키워드 가져오기
    file_prefix = korean_romanizer_converter(search_keyword)
    destination_file_name = f'{search_keyword}.parquet'
    key_path = "./key.json"
    bucket_name = "naver-placeid-crawler-data-lake"

    current_page = 0
    restaurant_per_page = 70
    all_data = []
    KST = pytz.timezone('Asia/Seoul')
    today = datetime.now(KST).date()
    date_path = today.strftime('%Y/%m/%d')

    try:
        while True:
            if current_page == 0:
                pagenation = 1
                print(f"pagenation: {pagenation}")
            else:
                pagenation = current_page * restaurant_per_page
                print(f"pagenation: {pagenation}")
            data = {
                "operationName": "getRestaurants",
                "variables": {
                    "useReverseGeocode": True,
                    "isNmap": False,
                    "restaurantListInput": {
                        "query": search_keyword,
                        "rank": "리뷰많은",
                        "x": "127.098619",
                        "y": "37.389844",
                        "start": pagenation,
                        "display": restaurant_per_page,
                        "isPcmap": True
                    },
                    "restaurantListFilterInput": {
                        "x": "127.098619",
                        "y": "37.389844",
                        "display": restaurant_per_page,
                        "start": pagenation,
                        "query": search_keyword,
                        "rank": "리뷰많은"
                    },
                    "reverseGeocodingInput": {
                        "x": "127.098619",
                        "y": "37.389844"
                    }
                },
                "query": "<GraphQL 쿼리 문자열>"
            }
            response = make_request_with_retry(url, headers, data)  # API 요청 수행
            if response and response.json()['data']['restaurants']['items']:
                items = response.json()['data']['restaurants']['items']
                file_name = f"{file_prefix}/{date_path}/page{current_page}_items.parquet"
                upload_to_gcs(items, bucket_name, file_name, key_path)  # 데이터를 GCS에 업로드
                all_data.extend(items)
                current_page += 1
                time.sleep(10)  # 페이지당 10초 간격으로 요청
            else:
                break

        df = pd.DataFrame(all_data)
        file_name = f"{file_prefix}/{date_path}/LOAD_{destination_file_name}"
        upload_to_gcs(df, bucket_name, file_name, key_path)  # 모든 데이터를 GCS에 업로드

    except Exception as ex:
        print(f"크롤링 에러 발생: {ex}")

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 9),  # 고정된 start_date 사용
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'naver_place_crawler',
    default_args=default_args,
    description='Naver Place Crawler DAG',
    schedule_interval=timedelta(days=1),
)

# 키워드를 가져오는 태스크
get_keyword_task = PythonOperator(
    task_id='get_keyword',
    python_callable=get_keyword,
    dag=dag,
)

# 크롤러를 실행하는 태스크
run_crawler_task = PythonOperator(
    task_id='run_crawler',
    python_callable=run_crawler,
    provide_context=True,
    dag=dag,
)

# 태스크 간의 의존성 설정
get_keyword_task >> run_crawler_task

