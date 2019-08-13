import pandas as pd
import numpy as np


def covert_to_datetime(columns):
    """Convert column to datetime

    Arguments:
        columns {array} -- Array of columns names
    """

    for column in columns:
        trips_data[column] = trips_data[column].map(
            lambda x: x.strip(" +0000 UTC"))
        trips_data[column] = pd.to_datetime(
            trips_data[column],
            errors='coerce').dt.tz_localize('UTC').dt.tz_convert('US/Mountain')


def fare_amount_to_dollars():
    """Create a column with the fare amount in dollars
    depending on the fare currency
    """

    conditions = [
        trips_data["Fare Currency"] == "CRC",
        trips_data["Fare Currency"] == "CLP",
        trips_data["Fare Currency"] == "MXN"
    ]
    choices = [596, 684, 19]

    trips_data["Exchange Rate"] = np.select(conditions, choices)
    trips_data["Fare Amount Dollars"] = trips_data["Fare Amount"] / \
        trips_data["Exchange Rate"]


# Here we read the data that uber gave us
trips_data = pd.read_csv("Uber Data/Rider/trips_data.csv")

# I just want to know about completed and fare split rides. I'm not interested
# about canceled or other rides status
trips_status = ["COMPLETED", "FARE_SPLIT"]
trips_data = trips_data[trips_data["Trip or Order Status"].isin(trips_status)]

# We are going to need the dates in datetime format
datetime_columns = ['Request Time', 'Begin Trip Time', 'Dropoff Time']
covert_to_datetime(datetime_columns)

# For some reason when transforming the columnns to datetime some value got to
# be null but for now lets not worry a lot
trips_data["Request Time New"] = np.where(pd.isnull(
    trips_data["Request Time"]), trips_data["Begin Trip Time"], trips_data["Request Time"])
trips_data = trips_data.drop(["Request Time"], axis=1)
trips_data["Request Time"] = trips_data["Request Time New"]
trips_data = trips_data.drop(["Request Time New"], axis=1)

# Lets create a day column from Request Time
trips_data["Request Day"] = trips_data["Request Time"].dt.weekday_name

# Column with the request hour
trips_data["Request Hour"] = trips_data["Begin Trip Time"].dt.hour

# Lest create a month column
trips_data["Date"] = trips_data['Request Time'].dt.floor(
    'd') - pd.offsets.MonthBegin(1)

# Create column o fare amount in dollars
fare_amount_to_dollars()

# Create column of distance in km
trips_data["Distance (km)"] = trips_data["Distance (miles)"]*1.60934

trips_data = trips_data[trips_data['Date'].notnull()]

# Average monthly fare amount
trips_data['Monthly Fare Amount Dollars'] = trips_data.groupby(
    "Date")['Fare Amount Dollars'].transform('sum')

trips_data.to_csv("Uber Data/Rider/trips_data_processed.csv", index=False)
