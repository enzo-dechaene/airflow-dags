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

environments = [
    k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
    k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_ETABLISSEMENT"),
    k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_ETABLISSEMENT_OK")
]

dag = DAG(
    'kubernetes_pod_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

create_pod = KubernetesPodOperator(
    dag=dag,
    task_id='create_pod',
    name='send_message_pulsar',
    image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
    cmds=['python', 'my_script.py'],
    env=environments,
    namespace='airflow',
    is_delete_operator_pod=True,
    get_logs=True,
)

create_pod
