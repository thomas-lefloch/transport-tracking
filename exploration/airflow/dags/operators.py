import time

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task


def wait(ms: int):
    time.sleep(ms / 1000)


@task
def other_wait(ms: int):
    time.sleep(ms / 1000)


# tâches éxecutés en parallèles
# o = Task
#     o2 - o4
# o1 -   \   - o6
#     o3 - o5
with DAG("operators_order") as dag:
    task_1 = PythonOperator(task_id="1", python_callable=wait, op_kwargs={"ms": 1000})
    task_2 = PythonOperator(task_id="2", python_callable=wait, op_kwargs={"ms": 2000})
    task_3 = PythonOperator(task_id="3", python_callable=wait, op_kwargs={"ms": 2000})
    task_4 = other_wait.override(task_id="4")(1000)
    task_5 = other_wait.override(task_id="5")(2000)
    task_6 = other_wait.override(task_id="6")(1000)

    task_1 >> task_2 >> task_4 >> task_6
    task_1 >> task_3 >> task_5 >> task_6
    task_5 << task_2

    # aussi valide
    # task_1 >> [task_2, task_3]
    # task_2 >> task_4
    # task_3 >> task_5
    # [task_4, task5] >> task_6
    # task_5 << task_2
