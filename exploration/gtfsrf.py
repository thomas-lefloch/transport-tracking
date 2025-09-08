import requests
from google.transit import gtfs_realtime_pb2
from datetime import datetime

trip_feed = gtfs_realtime_pb2.FeedMessage()
trip_update = requests.get("https://ara-api.enroute.mobi/rla/gtfs/trip-update")

print("TRIP_UPDATE: ")
trip_feed.ParseFromString(trip_update.content)

vehicle_in_trip_update = 0
print(trip_feed.header)
print(dir(trip_feed.entity[0]))
print(trip_feed.entity[0])
print(trip_feed.entity[-1])

earliest_arrival = float("inf")
earliest_departure = float("inf")
latest_arrival = 0
latest_departure = 0
delayed_trips = []

for entity in trip_feed.entity:
    for stop_update in entity.trip_update.stop_time_update:
        if stop_update.arrival.HasField("delay") or stop_update.departure.HasField(
            "delay"
        ):
            delayed_trips.append(entity)

        if stop_update.arrival.HasField("time"):
            # if earliest_arrival == None:
            #     ealiest_arrival = stop_update.arrival.time
            if stop_update.arrival.time < earliest_arrival:
                earliest_arrival = stop_update.arrival.time
            elif stop_update.arrival.time > latest_arrival:
                latest_arrival = stop_update.arrival.time
        if stop_update.departure.HasField("time"):
            # if earliest_departure == None:
            #     ealiest_departure = stop_update.departure.time
            if stop_update.departure.time < earliest_departure:
                earliest_departure = stop_update.departure.time
            elif stop_update.departure.time > latest_departure:
                latest_departure = stop_update.departure.time

    if entity.HasField("vehicle"):
        vehicle_in_trip_update += 1

print("Launched at", datetime.now())
print(
    "Earliest time: ",
    datetime.fromtimestamp(earliest_arrival),
    datetime.fromtimestamp(earliest_departure),
)
print(
    "latest time: ",
    datetime.fromtimestamp(latest_arrival),
    datetime.fromtimestamp(latest_departure),
)

print(f"Delayed trips ({len(delayed_trips)}):")
for trip in delayed_trips:
    print(trip.trip_update.trip_id)


vehicle_feed = gtfs_realtime_pb2.FeedMessage()
vehicle_positions = requests.get(
    "https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions"
)

print("\n\nVEHICLES:")
vehicle_feed.ParseFromString(vehicle_positions.content)
vehicle_count = 0
print(vehicle_feed.header)
print(dir(vehicle_feed.entity[0]))
print(vehicle_feed.entity[0])
for entity in vehicle_feed.entity:
    vehicle_count += 1

# Tous les vehicules ont l'air d'être à la fin de trip_update
print("trip :", vehicle_in_trip_update, "vehicles", vehicle_count)
# print(dir(vehicle_feed.entity[0]))
# print(vehicle_feed.entity[0].ListFields())
