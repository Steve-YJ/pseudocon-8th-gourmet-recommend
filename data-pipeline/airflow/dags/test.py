from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
) as dag:

    # 첫 번째 태스크
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # 두 번째 태스크
    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    # 세 번째 태스크
    t3 = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello, world!"',
    )

    # 태스크 의존성 설정
    t1 >> t2 >> t3
