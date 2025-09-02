import requests
from google.transit import gtfs_realtime_pb2

trip_feed = gtfs_realtime_pb2.FeedMessage()
trip_update = requests.get("https://ara-api.enroute.mobi/rla/gtfs/trip-update")

print("TRIP_UPDATE: ")
trip_feed.ParseFromString(trip_update.content)

vehicle_in_trip_update = 0
print(trip_feed.header)
print(dir(trip_feed.entity[0]))
print(trip_feed.entity[0])
print(trip_feed.entity[-1])

for entity in trip_feed.entity:
    if entity.HasField("vehicle"):
        vehicle_in_trip_update += 1

vehicle_feed = gtfs_realtime_pb2.FeedMessage()
vehicle_positions = requests.get(
    "https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions"
)

print("VEHICLES:")
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
