import duckdb
from airflow.sdk import task
from helpers import find_latest_static_data

from airflow import DAG


# calculer la moyenne des retards
# se connecter aux deux base de données
# avoir un liste stop_id | heure prévue | heure réel
# SELECT stop_id, heure_prevue, heure_reel FROM realtime_stop INNER JOIN stop_time ON stop_id
# faire les moyennes en fonction de measured_at
# Exporter ça dans un csv "measured_at, avg_delay"
# Mettre ça dans power BI
@task
def print_latest(ti=None):
    print(ti.xcom_pull(task_ids="find_latest_static_data", key="return_value"))


with DAG("avg_delay") as dag:
    find_latest_static_data() >> print_latest()
