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
def upload_to_gcs(data, bucket_name, destination_file_name):
    # Airflow Variable에서 key_path 값 가져오기
    key_path = Variable.get("gcp_key")

    KST = pytz.timezone('Asia/Seoul')
    today = datetime.now(KST).date()
    date_path = today.strftime('%Y/%m/%d')

    credentials = service_account.Credentials.from_service_account_info(key_path)
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
            "query": "query getRestaurants($restaurantListInput: RestaurantListInput, $restaurantListFilterInput: RestaurantListFilterInput, $reverseGeocodingInput: ReverseGeocodingInput, $useReverseGeocode: Boolean = false, $isNmap: Boolean = false) {\n  restaurants: restaurantList(input: $restaurantListInput) {\n    items {\n      apolloCacheId\n      coupon {\n        ...CouponItems\n        __typename\n      }\n      ...CommonBusinessItems\n      ...RestaurantBusinessItems\n      __typename\n    }\n    ...RestaurantCommonFields\n    optionsForMap {\n      ...OptionsForMap\n      __typename\n    }\n    nlu {\n      ...NluFields\n      __typename\n    }\n    searchGuide {\n      ...SearchGuide\n      __typename\n    }\n    __typename\n  }\n  filters: restaurantListFilter(input: $restaurantListFilterInput) {\n    ...RestaurantFilter\n    __typename\n  }\n  reverseGeocodingAddr(input: $reverseGeocodingInput) @include(if: $useReverseGeocode) {\n    ...ReverseGeocodingAddr\n    __typename\n  }\n}\n\nfragment OptionsForMap on OptionsForMap {\n  maxZoom\n  minZoom\n  includeMyLocation\n  maxIncludePoiCount\n  center\n  spotId\n  keepMapBounds\n  __typename\n}\n\nfragment NluFields on Nlu {\n  queryType\n  user {\n    gender\n    __typename\n  }\n  queryResult {\n    ptn0\n    ptn1\n    region\n    spot\n    tradeName\n    service\n    selectedRegion {\n      name\n      index\n      x\n      y\n      __typename\n    }\n    selectedRegionIndex\n    otherRegions {\n      name\n      index\n      __typename\n    }\n    property\n    keyword\n    queryType\n    nluQuery\n    businessType\n    cid\n    branch\n    forYou\n    franchise\n    titleKeyword\n    location {\n      x\n      y\n      default\n      longitude\n      latitude\n      dong\n      si\n      __typename\n    }\n    noRegionQuery\n    priority\n    showLocationBarFlag\n    themeId\n    filterBooking\n    repRegion\n    repSpot\n    dbQuery {\n      isDefault\n      name\n      type\n      getType\n      useFilter\n      hasComponents\n      __typename\n    }\n    type\n    category\n    menu\n    context\n    __typename\n  }\n  __typename\n}\n\nfragment SearchGuide on SearchGuide {\n  queryResults {\n    regions {\n      displayTitle\n      query\n      region {\n        rcode\n        __typename\n      }\n      __typename\n    }\n    isBusinessName\n    __typename\n  }\n  queryIndex\n  types\n  __typename\n}\n\nfragment ReverseGeocodingAddr on ReverseGeocodingResult {\n  rcode\n  region\n  __typename\n}\n\nfragment CouponItems on Coupon {\n  total\n  promotions {\n    promotionSeq\n    couponSeq\n    conditionType\n    image {\n      url\n      __typename\n    }\n    title\n    description\n    type\n    couponUseType\n    __typename\n  }\n  __typename\n}\n\nfragment CommonBusinessItems on BusinessSummary {\n  id\n  dbType\n  name\n  businessCategory\n  category\n  description\n  hasBooking\n  hasNPay\n  x\n  y\n  distance\n  imageUrl\n  imageCount\n  phone\n  virtualPhone\n  routeUrl\n  streetPanorama {\n    id\n    pan\n    tilt\n    lat\n    lon\n    __typename\n  }\n  roadAddress\n  address\n  commonAddress\n  blogCafeReviewCount\n  bookingReviewCount\n  totalReviewCount\n  bookingUrl\n  bookingBusinessId\n  talktalkUrl\n  detailCid {\n    c0\n    c1\n    c2\n    c3\n    __typename\n  }\n  options\n  promotionTitle\n  agencyId\n  businessHours\n  newOpening\n  markerId @include(if: $isNmap)\n  markerLabel @include(if: $isNmap) {\n    text\n    style\n    __typename\n  }\n  imageMarker @include(if: $isNmap) {\n    marker\n    markerSelected\n    __typename\n  }\n  __typename\n}\n\nfragment RestaurantFilter on RestaurantListFilterResult {\n  filters {\n    index\n    name\n    value\n    multiSelectable\n    defaultParams {\n      age\n      gender\n      day\n      time\n      __typename\n    }\n    items {\n      index\n      name\n      value\n      selected\n      representative\n      displayName\n      clickCode\n      laimCode\n      type\n      icon\n      __typename\n    }\n    __typename\n  }\n  votingKeywordList {\n    items {\n      name\n      value\n      icon\n      clickCode\n      __typename\n    }\n    menuItems {\n      name\n      value\n      icon\n      clickCode\n      __typename\n    }\n    total\n    __typename\n  }\n  optionKeywordList {\n    items {\n      name\n      value\n      icon\n      clickCode\n      __typename\n    }\n    total\n    __typename\n  }\n  __typename\n}\n\nfragment RestaurantCommonFields on RestaurantListResult {\n  restaurantCategory\n  queryString\n  siteSort\n  selectedFilter {\n    order\n    rank\n    tvProgram\n    region\n    brand\n    menu\n    food\n    mood\n    purpose\n    sortingOrder\n    takeout\n    orderBenefit\n    cafeFood\n    day\n    time\n    age\n    gender\n    myPreference\n    hasMyPreference\n    cafeMenu\n    cafeTheme\n    theme\n    voting\n    filterOpening\n    keywordFilter\n    property\n    realTimeBooking\n    __typename\n  }\n  rcodes\n  location {\n    sasX\n    sasY\n    __typename\n  }\n  total\n  __typename\n}\n\nfragment RestaurantBusinessItems on RestaurantListSummary {\n  categoryCodeList\n  visitorReviewCount\n  visitorReviewScore\n  imageUrls\n  bookingHubUrl\n  bookingHubButtonName\n  visitorImages {\n    id\n    reviewId\n    imageUrl\n    profileImageUrl\n    nickname\n    __typename\n  }\n  visitorReviews {\n    id\n    review\n    reviewId\n    __typename\n  }\n  foryouLabel\n  foryouTasteType\n  microReview\n  tags\n  priceCategory\n  broadcastInfo {\n    program\n    date\n    menu\n    __typename\n  }\n  michelinGuide {\n    year\n    star\n    comment\n    url\n    hasGrade\n    isBib\n    alternateText\n    hasExtraNew\n    region\n    __typename\n  }\n  broadcasts {\n    program\n    menu\n    episode\n    broadcast_date\n    __typename\n  }\n  tvcastId\n  naverBookingCategory\n  saveCount\n  uniqueBroadcasts\n  isDelivery\n  deliveryArea\n  isCvsDelivery\n  isTableOrder\n  isPreOrder\n  isTakeOut\n  bookingDisplayName\n  bookingVisitId\n  bookingPickupId\n  popularMenuImages {\n    name\n    price\n    bookingCount\n    menuUrl\n    menuListUrl\n    imageUrl\n    isPopular\n    usePanoramaImage\n    __typename\n  }\n  newBusinessHours {\n    status\n    description\n    __typename\n  }\n  baemin {\n    businessHours {\n      deliveryTime {\n        start\n        end\n        __typename\n      }\n      closeDate {\n        start\n        end\n        __typename\n      }\n      temporaryCloseDate {\n        start\n        end\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  yogiyo {\n    businessHours {\n      actualDeliveryTime {\n        start\n        end\n        __typename\n      }\n      bizHours {\n        start\n        end\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  realTimeBookingInfo {\n    description\n    hasMultipleBookingItems\n    bookingBusinessId\n    bookingUrl\n    itemId\n    itemName\n    timeSlots {\n      date\n      time\n      timeRaw\n      available\n      __typename\n    }\n    __typename\n  }\n  __typename\n}"
            }
            response = make_request_with_retry(url, headers, data)  # API 요청 수행
            if response and response.json()['data']['restaurants']['items']:
                items = response.json()['data']['restaurants']['items']
                file_name = f"{file_prefix}/{date_path}/page{current_page}_items.parquet"
                upload_to_gcs(items, bucket_name, file_name)  # 데이터를 GCS에 업로드
                all_data.extend(items)
                current_page += 1
                time.sleep(10)  # 페이지당 10초 간격으로 요청
            else:
                break

        df = pd.DataFrame(all_data)
        file_name = f"{file_prefix}/{date_path}/LOAD_{destination_file_name}"
        upload_to_gcs(df, bucket_name, file_name)  # 모든 데이터를 GCS에 업로드

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

