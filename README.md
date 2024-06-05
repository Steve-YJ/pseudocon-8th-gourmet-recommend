# pseudocon-8th-gourmet-recommend

---

## Airflow 사용법

---
Airflow 관련 자료와 코드를 확인하기 위해서는 아래 디렉토리로 이동한다.
```commandline
cd data-pipeline/airflow
```
### Production Environment
- Airflow version: 2.9.1, Executor: CeleryExecutor
- docker compose 설정파일: docker-compose.yaml
- DAG 파일 위치: Git Repo의 dags와 git-sync (30s interval)
 
Google Compute Engine의 prod-airflow 인스턴스에서 배포  
```commandline
cd /airflow
sudo docker compose up airflow-init
sudo docker compose up -d
```
필요 패키지 추가 방법
```commandline
sudo vim requirements.txt # 필요 패키지 추가
sudo docker compose up —build -d
```
### Test Environment
로컬에서 테스트를 위해서 LocalExecutor Airflow를 띄워놓고 사용한다.
- Airflow version: 2.9.1, Executor: LocalExecutor
- dockeR compose 설정파일: docker-compose-local.yaml
- DAG 파일 위치: Git Repo의 dags를 volume 설정
아래 명령어를 이용해서 배포후 사용
```commandline
docker compose -f docker-compose-local.yaml up airflow-init
docker compose -f docker-compose-local.yaml up -d
```

