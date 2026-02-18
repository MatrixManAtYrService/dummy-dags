"""
Hello World DAG - 64 parallel tasks with random memory/duration for dashboard testing.
Tasks are distributed across three pools based on task_num % 3.
"""
import random
import time
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
    description="64 parallel tasks distributed across three pools by mod 3",
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

    # Task distribution:
    # - 0mod3_size15: 0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57,60,63 (22 tasks, 15 slots)
    # - 1mod3_size10: 1,4,7,10,13,16,19,22,25,28,31,34,37,40,43,46,49,52,55,58,61 (21 tasks, 10 slots)
    # - 2mod3_size5:  2,5,8,11,14,17,20,23,26,29,32,35,38,41,44,47,50,53,56,59,62 (21 tasks, 5 slots)
