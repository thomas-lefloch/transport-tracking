import os

from airflow.sdk import task

DB_PATH = "data/data.duckdb"
"""chemin du fichier db depuis la racine airflow"""


@task
def find_latest_static_data():
    # static data are in data/gtfs_static_*/gtfs_static.duckdb
    os.chdir("data")
    folders = os.listdir(".")
    static_folders = filter(lambda x: "gtfs_static" in x, folders)
    return "data/" + max(static_folders, key=os.path.getmtime)
