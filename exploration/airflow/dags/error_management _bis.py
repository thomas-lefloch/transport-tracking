import random

from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import DAG, task


# Une pipeline qui génére un nombre aléatoirement entre 1 et 10
# si se nombre est supérieur <5 . tout va bien
# sinon erreur
# Le résultat est afficher dans un fichier exports/error_management.txt avec le status et le chiffre
# Le résultat est ensuite lu et logger
def generate_number() -> int:
    return random.randint(1, 3)


def allow_only_2(ti):
    if ti.xcom_pull(task_ids="number", key="return_value") != "2":
        return "write_bad_result"
    else:
        return "write_good_result"


def write_result(status: str, filename: str, task_instance):
    number = int(task_instance.xcom_pull(task_ids="number", key="return_value"))
    with open(filename, "a") as export_file:
        export_file.write(f"{status}, number {number}\n")


def parse_result(filename: str):
    with open(filename, "a") as file:
        file.write("bien joué !\n")


with DAG("error_management_bis") as dag:
    filename = "exports/exported.txt"
    gen_number = PythonOperator(task_id="number", python_callable=generate_number)
    # TODO: branchPythonOperator
    branch = allow_only_2()
    bad = PythonOperator(
        task_id="write_bad_result",
        python_callable=write_result,
        op_kwargs={"status": "bad", "filename": filename},
    )
    good = PythonOperator(
        task_id="write_good_result",
        python_callable=write_result,
        op_kwargs={"status": "good", "filename": filename},
    )
    result = PythonOperator(
        task_id="result", python_callable=parse_result, op_args=[filename]
    )

    gen_number >> branch >> [good, bad]
    good >> result
