from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="hello", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hellooifjdsfoijeo")

    @task()
    def airflow():
        print("airflow from ariflow")

    # Set dependencies between tasks
    hello >> airflow()
