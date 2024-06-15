from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'owner': 'youngjeon',
    'start_date': datetime(2024, 6, 14), # 6월 15일부터 작업 실행
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'daily_increment_raw_data',
    default_args=default_args,
    description='Daily data pipeline to load, transform and clean up data',
    schedule_interval='5 15 * * *', # 한국시각(UTC+09) 기준 00시 05분
    catchup=False,
) as dag:

    # Task 1: Load data from GCS to temporary table
    load_data = BigQueryInsertJobOperator(
        task_id='load_data',
        configuration={
            "query": {
                "query": """
                    LOAD DATA OVERWRITE place_id.temp_crawler_data
                    FROM FILES (
                      format = 'PARQUET',
                      uris = ['gs://naver-placeid-crawler-data-lake/{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y/%m/%d') }}/*.parquet']
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 2: Transform data and insert into raw_data table
    transform_data = BigQueryInsertJobOperator(
        task_id='transform_data',
        configuration={
            "query": {
                "query": """
                    INSERT INTO place_id.raw_data
                    SELECT
                      id AS place_id,
                      name AS shop_name,
                      businessCategory AS business_category,
                      category AS food_category,
                      x AS latitude,
                      y AS longitude,
                      phone AS restaurant_phone_num,
                      roadAddress AS road_address,
                      address AS address_detail,
                      commonAddress AS common_address,
                      IFNULL(REPLACE(blogCafeReviewCount, ',', ''), '0') AS blog_cafe_review_count,
                      IFNULL(REPLACE(visitorReviewCount, ',', ''), '0') AS visitor_review_count,
                      (IFNULL(SAFE_CAST(REPLACE(blogCafeReviewCount, ',', '') AS INT64), 0) + IFNULL(SAFE_CAST(REPLACE(visitorReviewCount, ',', '') AS INT64), 0)) AS total_review_count,
                      search_keyword
                    FROM place_id.temp_crawler_data;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 3: Drop temporary table
    drop_temp_table = BigQueryInsertJobOperator(
        task_id='drop_temp_table',
        configuration={
            "query": {
                "query": "DROP TABLE place_id.temp_crawler_data;",
                "useLegacySql": False,
            }
        },
    )

    load_data >> transform_data >> drop_temp_table

