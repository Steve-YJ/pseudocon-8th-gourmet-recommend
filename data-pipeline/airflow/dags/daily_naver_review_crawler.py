from datetime import timedelta, datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False, location='asia-northeast3')
bigquery_client = bigquery_hook.get_client()

def get_restaurant_ids(bq_review_table, bq_place_id_table, **context):
    """
    Bigquery에서 식당 목록 추출 후 다음 태스크로 전달
    - 기존에 수집하던 식당과 신규 수집 식당을 분리해서 조회
    - gcp connectioin에 Bigquery 권한 부여 되어 있음
    """

    # 기존에 수집하던 식당 목록 불러오기
    existed_res_query = f"""
        SELECT DISTINCT(place_id)
        FROM `{bq_review_table}`
    """,
    existed_res_query_job = bigquery_client.query(existed_res_query)
    existed_res_query_result = existed_res_query_job.result()
    existed_restaurant_ids = [row.place_id for row in existed_res_query_result]
    context['task_instance'].xcom_push(key='existed_restaurant_ids', value=existed_restaurant_ids)

    # 신규 식당 목록 불러오기
    new_res_query = f"""
        SELECT DISTINCT(place_id)
        FROM `{bq_place_id_table}`
        WHERE place_id NOT IN 
        (
            SELECT DISTINCT(place_id)
            FROM `{bq_review_table}`
        )
        LIMIT 30
    """
    new_res_query_job = bigquery_client.query(new_res_query)
    new_res_query_result = new_res_query_job.result()
    new_restaurant_ids = [row.place_id for row in new_res_query_result]
    context['task_instance'].xcom_push(key='new_restaurant_ids', value=new_restaurant_ids)
    ret = {
        'existed_restaurant_ids': existed_restaurant_ids,
        'new_restaurant_ids': new_restaurant_ids
    }
    return ret


def invoke_gcloud_functions(function_url, execution_date, restaurant_ids, is_new):
    """
    CloudFuntion의 HTTP 인증 호출을 하기 위한 id_token 가지고 와서 Function's URL을 호출
    - CloudFuntion에서는 endpoint와 audience가 function's URL로 동일
    - CloudFucntion 호출을 위해서 Compute Engine에 Cloud Run 호출 권한이 필요
    """
    import requests
    import json
    import ast

    import google.auth.transport.requests
    import google.oauth2.id_token

    audience = function_url
    endpoint = function_url

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
    # headers 설정
    headers = {
        "Authorization": f"Bearer {id_token}",
        "Content-Type": "application/json"
    }
    restaurant_ids = ast.literal_eval(restaurant_ids)
    for restaurant_id in restaurant_ids:
        # 요청 데이터 설정 (JSON 형식)
        data = {
            'execution_date': execution_date,
            'place_id': restaurant_id,
            'is_new': is_new
        }
        # JSON 데이터 변환
        json_data = json.dumps(data).encode('utf-8')
        print(json_data)
        # Cloud Function 호출
        response = requests.post(endpoint, data=json_data, headers=headers)
        # 응답 처리
        if response.status_code == 200:
            print(response.text)
        else:
            print(f"Error: {response.status_code} - {response.text}")


def gcs_to_bigquery(bq_review_table, gcs_bucket, gcs_prefix):
    """
    GCS에 적재된 파케이 Execution date의 파일들을 Bigquery에 Load하는 Task
    - LOAD DDL문을 빅쿼리에서 실행시키는 방식
    - gcp connection에 Bigquery 권한 부여 되어 있음
    """
    # 기존에 수집하던 식당 목록 불러오기
    load_query = f"""
        LOAD DATA INTO `{bq_review_table}`
        FROM FILES (
          format = 'PARQUET',
          uris = ['gs://{gcs_bucket}/{gcs_prefix}/*.parquet']);
    """,
    load_query_job = bigquery_client.query(load_query)
    result = load_query_job.result()
    return result.state


kst = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "jiyoung",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 11, tzinfo=kst),
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id="daily_naver_review_crawler",
        schedule_interval="0 2 * * *",
        default_args=default_args,
        max_active_tasks=1,
        max_active_runs=1,  # 동시 실행 제어
        catchup=False,  # 과거의 실행안된 Task를 실행
        tags=['daily', 'naver', 'review'],
        params={
            "bq_review_table": "pseudocon-24-summer.review.naver",
            "bq_place_id_table": "pseudocon-24-summer.place_id.search_query",
            "gcs_review_bucket": "naver-restaurant-review-data-lake",
            "function_url": "https://asia-northeast3-pseudocon-24-summer.cloudfunctions.net/get-reviews-and-upload-to-gcs"
        }
) as dag:
    get_restaurant_ids_task = PythonOperator(
        task_id="get_restaurant_ids_task",
        python_callable=get_restaurant_ids,
        op_kwargs={
            "bq_review_table": '{{ params.bq_review_table }}',
            "bq_place_id_table": '{{ params.bq_place_id_table }}',
        }
    )

    with TaskGroup(group_id="ingest_review_task_group") as ingest_review_tg:
        existed_restaurants_task = PythonOperator(
            task_id="existed_restaurants_task",
            python_callable=invoke_gcloud_functions,
            op_kwargs={
                "function_url": '{{ params.function_url }}',
                "execution_date": '{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}',
                "restaurant_ids": '{{ task_instance.xcom_pull(key="existed_restaurant_ids") }}',
                "is_new": False
            },
        )
        new_restaurants_task = PythonOperator(
            task_id="new_restaurants_task",
            python_callable=invoke_gcloud_functions,
            op_kwargs={
                "function_url": '{{ params.function_url }}',
                "execution_date": '{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}',
                "restaurant_ids": '{{ task_instance.xcom_pull(key="new_restaurant_ids") }}',
                "is_new": True
            },
        )

    gcs_to_bigquery = PythonOperator(
        task_id="gcs_to_bigquery",
        python_callable=gcs_to_bigquery,
        op_kwargs={
            "bq_review_table": '{{ params.bq_review_table }}',
            "gcs_bucket": '{{ params.gcs_review_bucket }}',
            "gcs_prefix": '{{ execution_date.in_timezone("Asia/Seoul").strftime("%Y/%m/%d") }}'
        }
    )

    get_restaurant_ids_task >> ingest_review_tg >> gcs_to_bigquery
