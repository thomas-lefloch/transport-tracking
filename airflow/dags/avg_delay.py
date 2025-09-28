import os
import statistics
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb
import helpers
from airflow.sdk import task

from airflow import DAG


@task
def avg_delay(ti=None):
    db_file = os.environ["AIRFLOW_HOME"] + "/" + helpers.DB_PATH
    # db_file = "airflow/warehouse/data.duckdb"
    results = []
    with duckdb.connect(db_file, read_only=True) as con:
        query = """
            SELECT stop_time.stop_id, 
                stop_time_rt.time AT TIME ZONE 'Europe/Paris',
                arrival_time,
                (stop_time_rt.time AT TIME ZONE 'Europe/Paris')::TIME -
                arrival_time AS delay 
            FROM stop_time_rt 
            INNER JOIN stop_time ON stop_time_rt.stop_id = stop_time.stop_id 
            WHERE stop_time_rt.trip_id = stop_time.trip_id; 
        """
        results = con.sql(query).fetchall()

    delays = []
    for res in results:
        # delays.append(realtime_seconds - scheduled_seconds)

    time = datetime.today().time()
    bucketed_minutes = 0 if time.minute < 30 else 30
    bucketed_time = time.replace(minute=bucketed_minutes, second=0, microsecond=0)

    csv_line = [
        time.date().__str__(),
        bucketed_time.__str__(),
        statistics.mean(delays).__str__(),
    ]

    export_file = os.environ["AIRFLOW_HOME"] + "/exports/avg_delay.csv"
    # export_file = "airflow/exports/avg_delay.csv"
    with open(export_file, "a") as csv:
        if csv.tell() == 0:  # header
            csv.write(str.join(",", ["date", "hour", "avg_delay_second"]) + "\n")
        csv.write(str.join(",", csv_line) + "\n")


with DAG("avg_delay", schedule="30 * * * *") as dag:
    avg_delay()


if __name__ == "__main__":
    avg_delay()
