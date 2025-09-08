import subprocess

import duckdb
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG


# Il existe surement une variable pour "/opt/airflow" mais je n'ai pas réussi à la trouver
@task.bash(cwd="/opt/airflow")
def download_static_gtfs_data():
    import time

    timestamp = int(time.time())
    zip_path = f"tmp/gtfs_static_{timestamp}.zip"
    gtfs_static_url = "https://chouette.enroute.mobi/api/v1/datas/OpendataRLA/gtfs.zip"

    # La derniere ligne écrite dans stdout sera envoyer en tant que xcom avec la clé "return_value"
    # On s'en sert pour transmettre le nom du fichier
    return f"curl -L --create-dirs -o {zip_path} {gtfs_static_url} && echo {zip_path}"


# unzip n'est pas installer dans l'image docker d'airflow
# on utilise donc python
@task
def unzip_static_gtfs_data(ti=None):
    zip_path: str = ti.xcom_pull(
        task_ids="download_static_gtfs_data", key="return_value"
    )
    gtfs_destination = zip_path[: zip_path.index(".")]

    from zipfile import ZipFile

    with ZipFile(zip_path) as zip:
        zip.extractall(path=gtfs_destination)

    return gtfs_destination


@task
def write_in_duckdb(ti=None):
    import os
    from pathlib import Path

    extracted_gtfs = ti.xcom_pull(task_ids="unzip_static_gtfs_data", key="return_value")
    filename = Path(extracted_gtfs).stem
    db_root = f"data/{filename}"
    if not os.path.isdir(db_root):
        os.makedirs(db_root)

    with duckdb.connect(f"{db_root}/gtfs_static.duckdb") as con:
        tables = [
            {"name": "route", "file": "routes.txt"},
            {"name": "stop", "file": "stops.txt"},
            {"name": "trip", "file": "trips.txt"},
            {"name": "stop_time", "file": "stop_times.txt"},
        ]
        stmt = ""
        for table in tables:
            stmt += f"CREATE TABLE {table["name"]} AS SELECT * FROM read_csv('{extracted_gtfs}/{table["file"]}', store_rejects = true);"

        con.sql(stmt)
        con.sql("FROM reject_errors;").write_csv(f"{db_root}/errors.csv")


with DAG("download_static_gtfs") as dag:
    download = download_static_gtfs_data()

    unzip = unzip_static_gtfs_data()

    db_write = write_in_duckdb()

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf tmp/",
        cwd="/opt/airflow",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    download >> unzip >> db_write >> cleanup
