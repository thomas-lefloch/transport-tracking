import random

from airflow.sdk import DAG, task

# Une pipeline qui génére un nombre aléatoirement entre 1 et 10
# si se nombre est supérieur <5 . tout va bien
# sinon erreur
# Le résultat est afficher dans un fichier exports/error_management.txt avec le status et le chiffre
# Le résultat est ensuite lu et logger


@task(task_id="number")
def generate_number() -> int:
    return random.randint(1, 3)


@task.branch()
def allow_only_2(number):
    if number != 2:
        return "write_bad_result"
    else:
        return "write_good_result"


@task
def write_result(status: str, filename: str, task_instance):
    number = int(task_instance.xcom_pull(task_ids="number", key="return_value"))
    with open(filename, "a") as export_file:
        export_file.write(f"{status}, number {number}\n")


@task
def parse_result(filename: str):
    with open(filename, "a") as file:
        file.write("bien joué !\n")


# TODO: retries and delay

with DAG("error_management") as dag:
    filename = "exports/exported.txt"
    number = generate_number()
    allow_2 = allow_only_2(number)  # number >> allow_2
    bad = write_result.override(task_id="write_bad_result")("bad", filename)
    good = write_result.override(task_id="write_good_result")("good", filename)
    allow_2 >> [bad, good]
    good >> parse_result(filename)
