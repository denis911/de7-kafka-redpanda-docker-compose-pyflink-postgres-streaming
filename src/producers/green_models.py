import json
import dataclasses
import pandas as pd

from dataclasses import dataclass


@dataclass
class Ride:
    lpep_pickup_datetime: int  # epoch milliseconds
    lpep_dropoff_datetime: int  # epoch milliseconds
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float


# Read the parquet file and keep only these columns:
# lpep_pickup_datetime
# lpep_dropoff_datetime
# PULocationID
# DOLocationID
# passenger_count
# trip_distance
# tip_amount
# total_amount


def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        # passenger_count=int(row['passenger_count']), # If NaN then kafka complains...
        passenger_count=int(0 if pd.isna(row['passenger_count']) else row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

    