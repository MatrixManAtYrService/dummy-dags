"""
Hello World DAG - 64 parallel tasks with random memory/duration for dashboard testing.
Tasks are distributed across three pools based on their number.
"""
import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s


def get_pool_for_task(task_num: int) -> str:
    """Determine which pool a task belongs to based on its number."""
    if task_num % 10 == 0 and task_num > 0:
        return "multiples-of-ten"
    elif task_num % 3 == 0 and task_num > 0:
        return "multiples-of-three"
    else:
        return "everything-else"


def make_executor_config(pool_name: str) -> dict:
    """Create executor_config with pod labels for the pool."""
    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={"pool": pool_name}
            )
        )
    }


def random_load(task_id: str):
    """Allocate random memory and hold it for random duration."""
    # Random memory: 0-512MB
    mem_mb = random.uniform(0, 512)
    mem_bytes = int(mem_mb * 1024 * 1024)

    # Random duration: 15s-2m
    duration = random.uniform(15, 120)

    print(f"Task {task_id}: Allocating {mem_mb:.2f}MB for {duration:.1f}s")

    # Allocate memory and hold it
    data = bytearray(mem_bytes)
    # Touch the memory to ensure it's actually allocated
    for i in range(0, len(data), 4096):
        data[i] = 1

    time.sleep(duration)
    print(f"Task {task_id}: Done")


with DAG(
    dag_id="hello_world",
    description="64 parallel tasks distributed across three pools",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["example", "stress-test"],
) as dag:

    tasks = []
    for i in range(64):
        pool_name = get_pool_for_task(i)
        task = PythonOperator(
            task_id=f"hello_{i:02d}",
            python_callable=random_load,
            op_args=[f"{i:02d}"],
            pool=pool_name,
            executor_config=make_executor_config(pool_name),
        )
        tasks.append(task)

    # All 64 tasks run in parallel (limited by pool slots)
    # - multiples-of-ten: 10, 20, 30, 40, 50, 60 (6 tasks, 10 slots)
    # - multiples-of-three: 3, 6, 9, 12, 15, 18, 21, 24, 27, 33, 36, 39, 42, 45, 48, 51, 54, 57, 63 (19 tasks, 8 slots)
    # - everything-else: 0, 1, 2, 4, 5, 7, 8, 11, 13, 14, 16, 17, 19, 22, 23, 25, 26, 28, 29, 31, 32, 34, 35, 37, 38, 41, 43, 44, 46, 47, 49, 52, 53, 55, 56, 58, 59, 61, 62 (39 tasks, 4 slots)
