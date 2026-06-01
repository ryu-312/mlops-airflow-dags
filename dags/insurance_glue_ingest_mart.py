from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

with DAG(
    dag_id="insurance_glue_ingest_mart",
    start_date=datetime(2026, 5, 29),
    schedule=None,
    catchup=False,
    tags=["mlops", "glue", "insurance"],
) as dag:

    ingest_raw = GlueJobOperator(
        task_id="ingest_raw_with_glue",
        job_name="mlops-insurance-ingest",
        aws_conn_id="aws_default",
        region_name="ap-northeast-2",
        script_args={
            "--input_path": "s3://s3-an2-mlops/landing/insurance/insurance.csv",
            "--raw_output_path": "s3://s3-an2-mlops/raw/insurance/",
            "--run_date": "{{ ds }}",
        },
        wait_for_completion=True,
        verbose=True,
    )

    build_mart = GlueJobOperator(
        task_id="build_mart_with_glue",
        job_name="mlops-insurance-mart",
        aws_conn_id="aws_default",
        region_name="ap-northeast-2",
        script_args={
            "--raw_input_path": "s3://s3-an2-mlops/raw/insurance/",
            "--mart_output_path": "s3://s3-an2-mlops/mart/insurance_features/",
            "--run_date": "{{ ds }}",
        },
        wait_for_completion=True,
        verbose=True,
    )

    repair_raw = AthenaOperator(
        task_id="repair_raw_partitions",
        query="MSCK REPAIR TABLE mlops.insurance_raw_glue",
        database="mlops",
        output_location="s3://s3-an2-mlops/athena/",
        aws_conn_id="aws_default",
    )

    repair_mart = AthenaOperator(
        task_id="repair_mart_partitions",
        query="MSCK REPAIR TABLE mlops.insurance_features_glue",
        database="mlops",
        output_location="s3://s3-an2-mlops/athena/",
        aws_conn_id="aws_default",
    )

    validate_mart = AthenaOperator(
        task_id="validate_mart",
        query="""
        SELECT count(*) AS row_count
        FROM mlops.insurance_features_glue
        WHERE run_dt = '{{ ds }}'
        """,
        database="mlops",
        output_location="s3://s3-an2-mlops/athena/",
        aws_conn_id="aws_default",
    )

    ingest_raw >> repair_raw >> build_mart >> repair_mart >> validate_mart
