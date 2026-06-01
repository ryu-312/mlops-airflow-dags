"""
DAG : dag_mrkt_evnt_pred_churn_train
설명 : mlops.mrkt_evnt_pred_churn_train 마트 월별 파티션 재적재
스케줄: 매월 1일 오전 6시 (전월 BS_YM 기준)

흐름:
  t1. S3에서 SQL 읽기 + BS_YM 계산
  t2. 기존 파티션 S3 데이터 삭제
  t3. Athena 파티션 메타 DROP
  t4. INSERT INTO 적재 (BS_YM 치환)
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
AWS_REGION     = 'ap-northeast-2'
ATHENA_DB      = 'mlops'
ATHENA_OUTPUT  = 's3://s3-an2-mlops/edwown/athena-results/'
SQL_BUCKET     = 's3-an2-mlops'
SQL_KEY        = 'sql/mrkt_evnt_pred_churn_train.sql'
MART_TABLE     = 'mrkt_evnt_pred_churn_train'
MART_BUCKET    = 's3-an2-mlops'
MART_PREFIX    = 'aimodel/mrkt_evnt_pred_churn_train'

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
def get_bs_ym(execution_date):
    """실행일 기준 당월 YYYYMM 반환 (매월 1일 실행 → 당월이 곧 적재 대상월)"""
    return execution_date.strftime('%Y%m')


def run_athena(sql, timeout=3600):
    """Athena 쿼리 실행 후 완료 대기. 성공 시 query_execution_id 반환"""
    athena = boto3.client('athena', region_name=AWS_REGION)
    resp = athena.start_query_execution(
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
                f"소요: {stats.get('TotalExecutionTimeInMillis',0)/1000:.1f}s | "
                f"스캔: {stats.get('DataScannedInBytes',0)/1024/1024:.1f}MB"
            )
            return qid
        if state in ('FAILED', 'CANCELLED'):
            reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
            raise Exception(f"[Athena] 실패 [{state}]: {reason}")

    raise TimeoutError(f"[Athena] 타임아웃 ({timeout}s 초과)")


def read_sql_from_s3():
    """S3에서 SQL 파일 읽기"""
    s3   = boto3.client('s3', region_name=AWS_REGION)
    body = s3.get_object(Bucket=SQL_BUCKET, Key=SQL_KEY)['Body'].read().decode('utf-8')
    logging.info(f"[S3] SQL 로드 완료: s3://{SQL_BUCKET}/{SQL_KEY} ({len(body)} chars)")
    return body


# ============================================================
# Task 함수
# ============================================================
def task_prepare(**context):
    """BS_YM 계산 + SQL 파일 읽어서 XCom 저장"""
    bs_ym     = get_bs_ym(context['execution_date'])
    sql_tmpl  = read_sql_from_s3()

    # ${bs_ym} → 실제 값으로 치환
    sql_final = sql_tmpl.replace('${bs_ym}', bs_ym)

    logging.info(f"[prepare] BS_YM = {bs_ym}")
    context['ti'].xcom_push(key='bs_ym',     value=bs_ym)
    context['ti'].xcom_push(key='sql_final', value=sql_final)


def task_delete_partition_s3(**context):
    """기존 파티션 S3 데이터 삭제"""
    bs_ym  = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    prefix = f"{MART_PREFIX}/bs_ym={bs_ym}/"
    logging.info(f"[S3 delete] s3://{MART_BUCKET}/{prefix}")

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

    logging.info(f"[S3 delete] 삭제 완료: {deleted}개 오브젝트")


def task_drop_partition_meta(**context):
    """Athena 파티션 메타데이터 DROP"""
    bs_ym = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    sql   = (
        f"ALTER TABLE {ATHENA_DB}.{MART_TABLE} "
        f"DROP IF EXISTS PARTITION (bs_ym='{bs_ym}')"
    )
    logging.info(f"[Athena DROP PARTITION] bs_ym={bs_ym}")
    run_athena(sql, timeout=120)
    logging.info(f"[Athena DROP PARTITION] 완료")


def task_insert(**context):
    """INSERT INTO 적재 (BS_YM 치환된 SQL 실행)"""
    bs_ym     = context['ti'].xcom_pull(key='bs_ym',     task_ids='prepare')
    sql_final = context['ti'].xcom_pull(key='sql_final', task_ids='prepare')

    logging.info(f"[INSERT] 적재 시작: bs_ym={bs_ym}")
    run_athena(sql_final, timeout=3600)
    logging.info(f"[INSERT] 적재 완료: bs_ym={bs_ym}")


def task_verify(**context):
    """적재 건수 검증 (0건이면 DAG 실패 처리)"""
    bs_ym = context['ti'].xcom_pull(key='bs_ym', task_ids='prepare')
    sql   = f"""
        SELECT COUNT(*) AS cnt
        FROM {ATHENA_DB}.{MART_TABLE}
        WHERE bs_ym = '{bs_ym}'
    """
    qid    = run_athena(sql, timeout=120)
    athena = boto3.client('athena', region_name=AWS_REGION)
    result = athena.get_query_results(QueryExecutionId=qid)
    cnt    = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    logging.info(f"[verify] bs_ym={bs_ym} | 적재건수={cnt:,}건")

    if cnt == 0:
        raise ValueError(f"[verify] bs_ym={bs_ym} 적재 건수 0건! 확인 필요")


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id='dag_mrkt_evnt_pred_churn_train',
    default_args=default_args,
    description='mrkt_evnt_pred_churn_train 마트 월별 파티션 재적재',
    schedule_interval='0 6 1 * *',   # 매월 1일 오전 6시
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'aimodel', 'mart', 'athena', 'churn'],
) as dag:

    t1 = PythonOperator(
        task_id='prepare',
        python_callable=task_prepare,
    )

    t2 = PythonOperator(
        task_id='delete_partition_s3',
        python_callable=task_delete_partition_s3,
    )

    t3 = PythonOperator(
        task_id='drop_partition_meta',
        python_callable=task_drop_partition_meta,
    )

    t4 = PythonOperator(
        task_id='insert',
        python_callable=task_insert,
    )

    t5 = PythonOperator(
        task_id='verify',
        python_callable=task_verify,
    )

    t1 >> t2 >> t3 >> t4 >> t5
