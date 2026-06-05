from datetime import datetime
from urllib.parse import urlparse

import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


DATABASE = "mlops"
ATHENA_OUTPUT = "s3://s3-an2-mlops/athena/"
FEATURE_TABLE = "insurance_features"
FEATURE_PATH = "s3://s3-an2-mlops/features/insurance/"

TRAINING_IMAGE = (
    "891376975666.dkr.ecr.ap-northeast-2.amazonaws.com/"
    "insurance-train:latest"
)


def delete_s3_prefix(s3_uri: str):
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    s3 = boto3.resource("s3", region_name="ap-northeast-2")
    bucket_obj = s3.Bucket(bucket)
    bucket_obj.objects.filter(Prefix=prefix).delete()


with DAG(
    dag_id="insurance_poc_e2e",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "insurance", "poc"],
) as dag:

    drop_feature_table = AthenaOperator(
        task_id="drop_feature_table",
        query=f"DROP TABLE IF EXISTS {DATABASE}.{FEATURE_TABLE}",
        database=DATABASE,
        output_location=ATHENA_OUTPUT,
        aws_conn_id="aws_default",
    )

    delete_feature_s3_prefix = PythonOperator(
        task_id="delete_feature_s3_prefix",
        python_callable=delete_s3_prefix,
        op_kwargs={"s3_uri": FEATURE_PATH},
    )

    create_feature_table = AthenaOperator(
        task_id="create_feature_table",
        query=f"""
        CREATE TABLE {DATABASE}.{FEATURE_TABLE}
        WITH (
          format = 'PARQUET',
          external_location = '{FEATURE_PATH}',
          write_compression = 'SNAPPY'
        ) AS
        SELECT
          age,
          bmi,
          children,
          CASE WHEN sex = 'male' THEN 1 ELSE 0 END AS sex_male,
          CASE WHEN smoker = 'yes' THEN 1 ELSE 0 END AS smoker_yes,
          CASE WHEN region = 'northeast' THEN 1 ELSE 0 END AS region_northeast,
          CASE WHEN region = 'northwest' THEN 1 ELSE 0 END AS region_northwest,
          CASE WHEN region = 'southeast' THEN 1 ELSE 0 END AS region_southeast,
          CASE WHEN region = 'southwest' THEN 1 ELSE 0 END AS region_southwest,
          charges
        FROM {DATABASE}.insurance_raw
        """,
        database=DATABASE,
        output_location=ATHENA_OUTPUT,
        aws_conn_id="aws_default",
    )

    # mlflow_auth_env = [
    #     k8s.V1EnvVar(
    #         name="MLFLOW_TRACKING_USERNAME",
    #         value_from=k8s.V1EnvVarSource(
    #             secret_key_ref=k8s.V1SecretKeySelector(
    #                 name="mlflow-tracking-auth",
    #                 key="MLFLOW_TRACKING_USERNAME",
    #             )
    #         ),
    #     ),
    #     k8s.V1EnvVar(
    #         name="MLFLOW_TRACKING_PASSWORD",
    #         value_from=k8s.V1EnvVarSource(
    #             secret_key_ref=k8s.V1SecretKeySelector(
    #                 name="mlflow-tracking-auth",
    #                 key="MLFLOW_TRACKING_PASSWORD",
    #             )
    #         ),
    #     ),
    # ]

    runtime_env_vars = [
        k8s.V1EnvVar(
            name="AWS_REGION",
            value="ap-northeast-2",
        ),
        k8s.V1EnvVar(
            name="AWS_DEFAULT_REGION",
            value="ap-northeast-2",
        ),
        k8s.V1EnvVar(
            name="MLFLOW_TRACKING_URI",
            value="http://mlflow.mlflow.svc.cluster.local:80",
        ),
        k8s.V1EnvVar(
            name="MLFLOW_TRACKING_USERNAME",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="mlflow-tracking-auth",
                    key="MLFLOW_TRACKING_USERNAME",
                )
            ),
        ),
        k8s.V1EnvVar(
            name="MLFLOW_TRACKING_PASSWORD",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="mlflow-tracking-auth",
                    key="MLFLOW_TRACKING_PASSWORD",
                )
            ),
        ),
    ]
    
    batch_predict = KubernetesPodOperator(
        task_id="batch_predict_in_b_eks",
        name="insurance-batch-predict",
        namespace="mlops",
    
        image="891376975666.dkr.ecr.ap-northeast-2.amazonaws.com/insurance-inference:latest",
        cmds=["python", "batch_predict.py"],
        arguments=[
            "--input-path",
            "s3://s3-b-mlops/batch-input/insurance/run_dt={{ ds }}/input.csv",
            "--output-path",
            "s3://s3-b-mlops/batch-output/insurance/run_dt={{ ds }}/predictions.parquet",
            "--model-uri",
            "models:/insurance-charges-model@champion",
        ],
    
        env_vars=runtime_env_vars,
    
        service_account_name="mlops-runtime",
    
        in_cluster=False,
        config_file="/opt/airflow/kubeconfigs/config",
        cluster_context="arn:aws:eks:ap-northeast-2:891376975666:cluster/eks-airflow-test",
    
        get_logs=True,
        is_delete_operator_pod=False,
    )

    # train_model = KubernetesPodOperator(
    #     task_id="train_model",
    #     name="insurance-train",
    #     namespace="airflow",
    #     image=TRAINING_IMAGE,
    #     cmds=["python", "train.py"],
    #     arguments=[
    #         "--feature-table", FEATURE_TABLE,
    #         "--athena-database", DATABASE,
    #         "--athena-output", ATHENA_OUTPUT,
    #         "--experiment-name", "insurance-poc",
    #         "--registered-model-name", "insurance-charges-model",
    #         "--min-r2", "0.7",
    #     ],
    #     env_vars={
    #         "AWS_REGION": "ap-northeast-2",
    #         "AWS_DEFAULT_REGION": "ap-northeast-2",
    #         "MLFLOW_TRACKING_URI": "http://mlflow.mlflow.svc.cluster.local:80",
    #     },
    #     full_pod_spec=k8s.V1Pod(
    #         spec=k8s.V1PodSpec(
    #             containers=[
    #                 k8s.V1Container(
    #                     name="base",
    #                     image=TRAINING_IMAGE,
    #                     env=mlflow_auth_env,
    #                 )
    #             ]
    #         )
    #     ),
    #     service_account_name="mlops-training",
    #     get_logs=True,
    #     is_delete_operator_pod=False,
    #     on_finish_action="keep_pod",
    # )

    (
        drop_feature_table
        >> delete_feature_s3_prefix
        >> create_feature_table
        # >> train_model
        >> batch_predict
    )
