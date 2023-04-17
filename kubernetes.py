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

environments_etab = [
    k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
    k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_ETABLISSEMENT"),
    k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_ETABLISSEMENT_OK")
]

environments_userdata = [
    k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
    k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_USERDATA"),
    k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_USERDATA_OK")
]

environments_mobilite_auth = [
    k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
    k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_MOBILITE_AUTH_AUTH"),
    k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_MOBILITE_AUTH_AUTH_OK")
]

environments_utilisateur = [
    k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
    k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_UTILISATEUR"),
    k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_UTILISATEUR_OK")
]

DAG_ID = "pulsar"

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
        name='send_message_pulsar_etablissement',
        namespace='airflow',
        image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
        env_vars=environments_etab,
        task_id="task-one",
    )

    t2 = KubernetesPodOperator(
        dag=dag,
        name='send_message_pulsar_2',
        namespace='airflow',
        image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
        env_vars=environments_userdata,
        task_id="task-two",
    )

    t3 = KubernetesPodOperator(
        dag=dag,
        name='send_message_pulsar_3',
        namespace='airflow',
        image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
        env_vars=environments_mobilite_auth,
        task_id="task-three",
    )

    t4 = KubernetesPodOperator(
        dag=dag,
        name='send_message_pulsar_4',
        namespace='airflow',
        image='harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11',
        env_vars=environments_utilisateur,
        task_id="task-four",
    )

    [t1, t2, t3] >> t4 