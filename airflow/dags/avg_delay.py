import os

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
def avg_delay(ti=None):
    db_file = os.environ["AIRFLOW_HOME"] + "/warehouse/data.duckdb"
    results = []
    with duckdb.connect(db_file, read_only=True) as con:
        results = con.sql(
            "SELECT stop_time.stop_id, stop_time.trip_id as scheduled_trip_id, stop_time_rt.trip_id as realtime_trip_id, stop_time_rt.time - arrival_time::INTERVAL AS delay FROM stop_time_rt INNER JOIN stop_time ON stop_time_rt.stop_id = stop_time.stop_id WHERE stop_time_rt.trip_id = stop_time.trip_id;"
        )

    # for res in results:


with DAG("avg_delay", schedule="30 * * * *") as dag:
    avg_delay()


if __name__ == "__main__":
    avg_delay()
