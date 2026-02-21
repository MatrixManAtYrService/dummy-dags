"""
Hello World DAG - 64 parallel tasks calling the dummy greeting service.
Tasks are distributed across three pools based on task_num % 3.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s


# Pool definitions: task_num % 3 determines pool
POOLS = {
    0: "0mod3_size15",  # 15 slots
    1: "1mod3_size10",  # 10 slots
    2: "2mod3_size5",   # 5 slots
}


def get_pool_for_task(task_num: int) -> str:
    """Determine which pool a task belongs to based on task_num % 3."""
    return POOLS[task_num % 3]


def make_executor_config(pool_name: str) -> dict:
    """Create executor_config with pod labels for the pool."""
    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={"pool": pool_name}
            )
        )
    }


def call_greeting_service(task_num: str):
    """Call the dummy greeting service via the client library."""
    from airflow_client_lib.client import get_base_url, greet

    base_url = get_base_url("dummy_server")
    name = f"hello_{task_num}"
    print(f"Task {task_num}: calling {base_url} with name={name}")
    greeting = greet(base_url, name)
    print(f"Task {task_num}: got response: {greeting}")


with DAG(
    dag_id="hello_world",
    description="64 parallel tasks calling the dummy greeting service via client lib",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["example", "service-test"],
) as dag:

    tasks = []
    for i in range(64):
        pool_name = get_pool_for_task(i)
        task = PythonOperator(
            task_id=f"hello_{i:02d}",
            python_callable=call_greeting_service,
            op_args=[f"{i:02d}"],
            pool=pool_name,
            executor_config=make_executor_config(pool_name),
        )
        tasks.append(task)
