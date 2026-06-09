"""
DAG : dag_mrkt_evnt_pred_churn_train
설명 : mlops.mrkt_evnt_pred_churn_train 마트 월별 파티션 재적재
스케줄: 매월 1일 오전 6시
       (테스트용: BS_YM 하드코딩 '202604')

흐름:
  t1. BS_YM 결정 (하드코딩)
  t2. 기존 파티션 S3 데이터 삭제
  t3. Athena 파티션 메타 DROP
  t4. S3 SQL 읽어서 그대로 실행 (치환 없음)
  t5. 적재 건수 검증
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time
import logging

# ============================================================
# 설정
# ============================================================
AWS_REGION    = 'ap-northeast-2'
ATHENA_DB     = 'mlops'
ATHENA_OUTPUT = 's3://s3-an2-mlops/edwown/athena-results/'
SQL_BUCKET    = 's3-an2-mlops'
SQL_KEY       = 'sql/mrkt_evnt_pred_churn_train.sql'
MART_TABLE    = 'mrkt_evnt_pred_churn_train'
MART_BUCKET   = 's3-an2-mlops'
MART_PREFIX   = 'aimodel/mrkt_evnt_pred_churn_train'

# 테스트용 하드코딩 BS_YM
BS_YM_HARDCODED = '202604'

default_args = {
    'owner'           : 'mlops',
    'depends_on_past' : False,
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
}


# ============================================================
# 공통 유틸
# ============================================================
def run_athena(sql, timeout=3600):
    athena = boto3.client('athena', region_name=AWS_REGION)
    resp   = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
    )
    qid = resp['QueryExecutionId']
    logging.info(f"[Athena] Query ID: {qid}")

    elapsed = 0
    while elapsed < timeout:
        time.sleep(5)
        elapsed += 5
        status = athena.get_query_execution(QueryExecutionId=qid)
        state  = status['QueryExecution']['Status']['State']
        if elapsed % 30 == 0:
            logging.info(f"[Athena] {elapsed}s 경과 / 상태: {state}")
        if state == 'SUCCEEDED':
            stats = status['QueryExecution'].get('Statistics', {})
            logging.info(
                f"[Athena] 완료 | "
                f"소요: {stats.get('TotalExecutionTimeInMillis', 0) / 1000:.1f}s | "
                f"스캔: {stats.get('DataScannedInBytes', 0) / 1024 / 1024:.1f}MB"
            )
            return qid
        if state in ('FAILED', 'CANCELLED'):
            reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
            raise Exception(f"[Athena] 실패 [{state}]: {reason}")

    raise TimeoutError(f"[Athena] 타임아웃 ({timeout}s 초과)")


def read_sql_from_s3():
    """S3에서 SQL 파일 그대로 읽기 (치환 없음)"""
    s3   = boto3.client('s3', region_name=AWS_REGION)
    body = s3.get_object(Bucket=SQL_BUCKET, Key=SQL_KEY)['Body'].read().decode('utf-8')
    logging.info(f"[S3] SQL 로드 완료: s3://{SQL_BUCKET}/{SQL_KEY} ({len(body)} chars)")
    return body


# ============================================================
# Task 함수
# ============================================================
def task_prepare(**context):
    """BS_YM 하드코딩 값 사용"""
    bs_ym = BS_YM_HARDCODED
    logging.info(f"[prepare] BS_YM = {bs_ym} (하드코딩)")
    context['ti'].xcom_push(key='bs_ym', value=bs_ym)


def task_delete_partition_s3(**context):
    """기존 파티션 S3 데이터 삭제"""
    bs_ym  = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    prefix = f"{MART_PREFIX}/bs_ym={bs_ym}/"
    logging.info(f"[S3 delete] 시작: s3://{MART_BUCKET}/{prefix}")

    s3        = boto3.client('s3', region_name=AWS_REGION)
    paginator = s3.get_paginator('list_objects_v2')
    deleted   = 0
    for page in paginator.paginate(Bucket=MART_BUCKET, Prefix=prefix):
        objs = page.get('Contents', [])
        if objs:
            s3.delete_objects(
                Bucket=MART_BUCKET,
                Delete={'Objects': [{'Key': o['Key']} for o in objs]},
            )
            deleted += len(objs)
    logging.info(f"[S3 delete] 완료: {deleted}개 삭제")


def task_drop_partition_meta(**context):
    """Athena 파티션 메타데이터 DROP"""
    bs_ym = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    sql   = (
        f"ALTER TABLE {ATHENA_DB}.{MART_TABLE} "
        f"DROP IF EXISTS PARTITION (bs_ym='{bs_ym}')"
    )
    logging.info(f"[DROP PARTITION] bs_ym={bs_ym}")
    run_athena(sql, timeout=120)


def task_insert(**context):
    """S3에서 SQL 그대로 읽어서 INSERT 실행 (하드코딩 SQL이므로 치환 불필요)"""
    bs_ym = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    sql   = read_sql_from_s3()

    logging.info(f"[INSERT] 적재 시작: bs_ym={bs_ym}")
    run_athena(sql, timeout=3600)
    logging.info(f"[INSERT] 적재 완료: bs_ym={bs_ym}")


def task_verify(**context):
    """적재 건수 검증"""
    bs_ym  = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    sql    = f"SELECT COUNT(*) AS cnt FROM {ATHENA_DB}.{MART_TABLE} WHERE bs_ym = '{bs_ym}'"
    qid    = run_athena(sql, timeout=120)
    athena = boto3.client('athena', region_name=AWS_REGION)
    result = athena.get_query_results(QueryExecutionId=qid)
    cnt    = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    logging.info(f"[verify] bs_ym={bs_ym} | 적재건수={cnt:,}건")
    if cnt == 0:
        raise ValueError(f"[verify] bs_ym={bs_ym} 적재 건수 0건!")


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id='mrkt_evnt_pred_churn_train',
    default_args=default_args,
    description='mrkt_evnt_pred_churn_train 마트 월별 파티션 재적재 (하드코딩 테스트)',
    schedule_interval=None,
    start_date=datetime(2026, 5, 31),
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'aimodel', 'mart', 'athena', 'churn'],
) as dag:

    t1 = PythonOperator(task_id='prepare',             python_callable=task_prepare,             provide_context=True)
    t2 = PythonOperator(task_id='delete_partition_s3', python_callable=task_delete_partition_s3, provide_context=True)
    t3 = PythonOperator(task_id='drop_partition_meta', python_callable=task_drop_partition_meta, provide_context=True)
    t4 = PythonOperator(task_id='insert',              python_callable=task_insert,              provide_context=True)
    t5 = PythonOperator(task_id='verify',              python_callable=task_verify,              provide_context=True)

    t1 >> t2 >> t3 >> t4 >> t5
