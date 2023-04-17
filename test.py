
from __future__ import annotations

import logging
import os

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

log = logging.getLogger(__name__)

worker_container_repository = conf.get("kubernetes_executor", "worker_container_repository")
worker_container_tag = conf.get("kubernetes_executor", "worker_container_tag")

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning(
        "The example_kubernetes_executor example DAG requires the kubernetes provider."
        " Please install it with: pip install apache-airflow[cncf.kubernetes]"
    )
    k8s = None


if k8s:
    with DAG(
        dag_id="example_kubernetes_executor",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["kubernetes"],
    ) as dag:
        # You can use annotations on your kubernetes pods!
        start_task_executor_config = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
        }

        @task(executor_config=start_task_executor_config)
        def start_task():
            print_stuff()

        # test my test
        init_environments = [
            k8s.V1EnvVar(name="PULSAR_URL", value="pulsar://172.16.106.50:30767"), 
            k8s.V1EnvVar(name="PULSAR_INPUT_MESSAGE", value="GENERATION_ETABLISSEMENT"),
            k8s.V1EnvVar(name="PULSAR_OUTPUT_MESSAGE", value="GENERATION_ETABLISSEMENT_OK")
        ]

        executor_config_pulsar = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="main",
                            image="harbor.knada.rancher.kosmos.fr/public/send_message_pulsar:v1.0.11",
                            env=init_environments,
                        ),
                    ],
                )
            ),
        }

        @task(executor_config=executor_config_pulsar)
        def test_volume_mount():
            """
            Tests whether the volume has been mounted.
            """

        volume_task = test_volume_mount()
        # END my test

        (
            start_task()
            >> [volume_task]
        )