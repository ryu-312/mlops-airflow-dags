from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


IMAGE = "891376975666.dkr.ecr.ap-northeast-2.amazonaws.com/insurance-inference:latest"

with DAG(
    dag_id="insurance_batch_inference",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "insurance", "batch"],
) as dag:

    batch_predict = KubernetesPodOperator(
        task_id="batch_predict",
        name="insurance-batch-predict",
        namespace="mlops",
        image=IMAGE,
        cmds=["python", "batch_predict.py"],
        arguments=[
            "--input-path",
            "s3://s3-an2-mlops/batch-input/insurance/run_dt={{ ds }}/input.csv",
            "--output-path",
            "s3://s3-an2-mlops/batch-output/insurance/run_dt={{ ds }}/predictions.parquet",
            "--model-uri",
            "models:/insurance-charges-model@champion",
        ],
        env_vars={
            "AWS_REGION": "ap-northeast-2",
            "AWS_DEFAULT_REGION": "ap-northeast-2",
            "MLFLOW_TRACKING_URI": "http://mlflow.mlflow.svc.cluster.local:80",
            "MLFLOW_TRACKING_USERNAME": "admin",
            "MLFLOW_TRACKING_PASSWORD": "Clkyobo11111!",
        },
        service_account_name="mlops-training",
        get_logs=True,
        is_delete_operator_pod=False,
    )
