from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-1")),
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-2")),
]

environments = [
    k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
    k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_ETABLISSEMENT"),
    k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_ETABLISSEMENT_OK")
]


with DAG(
    DAG_ID,
    default_args=default_args,
    description="pulsar",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    tags=["example", "cncf", "kubernetes"],
    catchup=False,
) as dag:
    t1 = KubernetesPodOperator(
        dag=dag,
        name='send_message_pulsar_1',
        namespace='airflow',
        image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
        image_pull_secrets=[k8s.V1LocalObjectReference("harbor-knada-credential")],
        env_from=environments,
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        task_id="task-one",
    )

    t2 = KubernetesPodOperator(
        dag=dag,
        name='send_message_pulsar_2',
        namespace='airflow',
        image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
        image_pull_secrets=[k8s.V1LocalObjectReference("harbor-knada-credential")],
        env_from=environments,
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        task_id="task-two",
    )

    t1 >> t2