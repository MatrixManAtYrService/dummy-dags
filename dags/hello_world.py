"""
Hello World DAG - 64 parallel tasks to stress test Celery.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_world",
    description="64 parallel bash tasks to test Celery queuing",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["example", "stress-test"],
) as dag:

    tasks = []
    for i in range(64):
        task = BashOperator(
            task_id=f"hello_{i:02d}",
            bash_command=f'echo "Hello from task {i:02d}!" && sleep 2',
            pool="stress_test",
        )
        tasks.append(task)

    # All 64 tasks run in parallel (limited by stress_test pool slots)
