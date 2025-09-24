import os
import time

import duckdb
import requests
from airflow.sdk import task
from google.transit import gtfs_realtime_pb2

from airflow import DAG

data_url = "https://ara-api.enroute.mobi/rla/gtfs/trip-update"
stop_time_tablename = "stop_time_rt"
vehicle_tablename = "vehicle"
db_file = f"{os.environ["AIRFLOW_HOME"]}/warehouse/data.duckdb"


def parse_vehicle(vehicle) -> dict:
    return {
        "trip_id": vehicle.trip.trip_id,
        "stop_id": vehicle.stop_id,
        "vehicle_id": vehicle.vehicle.id,
        "time": vehicle.timestamp * 1000000,
        "lat": vehicle.position.latitude,
        "long": vehicle.position.longitude,
        "bearing": vehicle.position.bearing,
    }


def parse_stop_time(stop_time) -> dict:
    bus_time = None
    if stop_time.HasField("arrival") and stop_time.arrival.HasField("time"):
        bus_time = stop_time.arrival.time * 1000000
    elif stop_time.HasField("departure") and stop_time.departure.HasField("time"):
        bus_time = stop_time.departure.time * 1000000

    return {
        "stop_id": stop_time.stop_id,
        "stop_sequence": stop_time.stop_sequence,
        "time": bus_time,
    }


@task(retries=3)
def fetch_and_parse_content():
    feed = gtfs_realtime_pb2.FeedMessage()
    req = requests.get(data_url)
    feed.ParseFromString(req.content)

    measured_at = int(time.time()) * 1000000  # duckdb is expecting millisceonds
    stop_time_data = []
    vehicle_data = []

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            data = {"measured_at": measured_at, **parse_vehicle(entity.vehicle)}
            vehicle_data.append(data)

        elif entity.HasField("trip_update"):
            for stop_time in entity.trip_update.stop_time_update:
                data = {
                    "measured_at": measured_at,
                    "trip_id": entity.trip_update.trip.trip_id,
                    **parse_stop_time(stop_time),
                }
                stop_time_data.append(data)

    with setup_duckdb(db_file) as con:
        insert_stop_time = f"""
            INSERT INTO {stop_time_tablename} 
            VALUES (
                $trip_id, 
                make_timestamp($measured_at), 
                $stop_id, 
                $stop_sequence, 
                make_timestamp($time)
            );
        """
        con.executemany(insert_stop_time, stop_time_data)

        insert_vehicles = f"""
            INSERT INTO {vehicle_tablename}
            VALUES (
                $trip_id, 
                make_timestamp($measured_at), 
                $stop_id, 
                $vehicle_id,
                make_timestamp($time),
                $lat,
                $long,
                $bearing
            );
        """
        con.executemany(insert_vehicles, vehicle_data)


# Se connecte et crée la table si nécessaire
def setup_duckdb(filename: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(filename)

    create_table_stmt = f"""
        CREATE TABLE IF NOT EXISTS {stop_time_tablename} (
            trip_id VARCHAR NOT NULL,
            measured_at TIMESTAMP WITH TIME ZONE NOT NULL,
            stop_id VARCHAR NOT NULL,
            stop_sequence INT NOT NULL,
            time TIMESTAMP WITH TIME ZONE
        );
        
        CREATE TABLE IF NOT EXISTS {vehicle_tablename} (
            trip_id VARCHAR NOT NULL,
            measured_at TIMESTAMP WITH TIME ZONE NOT NULL,
            stop_id VARCHAR NOT NULL,
            vehicle_id VARCHAR NOT NULL,
            time TIMESTAMP WITH TIME ZONE NOT NULL,
            lat FLOAT NOT NULL,
            long FLOAT NOT NULL,
            bearing INTEGER NOT NULL
        );
    """

    con.execute(create_table_stmt)
    return con


# every 30 minutes */30 * * * *
# Fetch data from realtime flux and append it to duckdb
with DAG("download_realtime_gtfs_data", schedule="20 * * * *") as dag:
    fetch_and_parse_content()

# cleanup: archive db every week in warehouse with week number
with DAG("archive_realtime_gtfs_data", schedule="59 23 * * 0") as dag:
    pass
