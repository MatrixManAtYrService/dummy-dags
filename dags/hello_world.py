"""
Hello World DAG - confirms git-sync is working.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_hello():
    print("Hello from git-synced DAG!")
    return "Hello World"


with DAG(
    dag_id="hello_world",
    description="A simple DAG to verify git-sync is working",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["example", "git-sync"],
) as dag:

    hello_bash = BashOperator(
        task_id="hello_bash",
        bash_command='echo "Hello from BashOperator! DAG synced from git."',
    )

    hello_python = PythonOperator(
        task_id="hello_python",
        python_callable=print_hello,
    )

    hello_bash >> hello_python
