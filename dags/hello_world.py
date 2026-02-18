"""
Hello World DAG - 64 parallel tasks with random memory/duration for dashboard testing.
"""
import random
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


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
    description="64 parallel tasks with random memory/duration",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["example", "stress-test"],
) as dag:

    tasks = []
    for i in range(64):
        task = PythonOperator(
            task_id=f"hello_{i:02d}",
            python_callable=random_load,
            op_args=[f"{i:02d}"],
            pool="stress_test",
        )
        tasks.append(task)

    # All 64 tasks run in parallel (limited by stress_test pool slots)
