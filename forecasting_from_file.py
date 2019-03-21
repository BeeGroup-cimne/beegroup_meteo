# -*- coding: utf-8 -*-
# Library imports
import json
import pandas as pd
import numpy as np
import pytz
from pymongo import MongoClient, DESCENDING
from dateutil.relativedelta import relativedelta
import os
import glob
import utils

def resampling_forecasting(x):
    try:
        time_x = x.name
        x = x.drop_duplicates("timeForecasting")
        x = x.set_index("timeForecasting")
        x = x.reindex(pd.DatetimeIndex(start=time_x-relativedelta(hours=48),end=time_x, freq="H"))
        x = x.fillna(method="bfill")
        x = x.resample("H").mean().interpolate()
    except:
        print ("{} Failed when resampling!".format(x.name))
    return (x)


working_directory = os.path.dirname(os.path.abspath(__file__))
with open('general_config.json') as f:
    config = json.load(f)
working_directory = config['data_directory']
for config in glob.glob('{}/available_config/*.json'.format(working_directory)):
    with open(config) as f:
        params = json.load(f)

    # Check if the forecasting is needed for this configuration
    if not 'forecasting' in params:
        exit(0)
    # Mongo connection
    client = MongoClient(params['mongodb']['host'], int(params['mongodb']['port']))
    client[params['mongodb']['db']].authenticate(
        params['mongodb']['username'],
        params['mongodb']['password']
    )
    mongo = client[params['mongodb']['db']]

    # Create the location list to download
    try:
        locations = utils.read_locations(params['forecasting'])
    except Exception as e:
        print("Unable to load locations for config {}: {}".format(config, e))
        continue

    # Download the data and upload it to Mongo
    for loc in locations:
        stationId = loc[0]
        print("Weather forecasting data for stationId {}".format(stationId))
        try:
            station_info = mongo[params['mongodb']["stations_collection"]].find_one({"stationId": stationId})
            ts = pytz.UTC.localize(station_info["forecasting_time"])
            ts -= relativedelta(hours=96)
        except:
            ts = None

        # Read the meteo forecastings
        r = pd.read_csv("{}/meteo_data/{:.2f}_{:.2f}_forecasting_hourly.csv".format(working_directory, loc[1], loc[2]))

        # Rearrange the time columns
        r.time = pd.to_datetime(r.time).dt.tz_localize(pytz.UTC)
        r.timeForecasting = pd.to_datetime(r.timeForecasting).dt.tz_localize(pytz.UTC)

        # Only get the last
        if ts:
            r = r[r.time >= ts]

        # Resample the timeForecasting to hourly.
        r = r.groupby(["time"]).apply(lambda x: resampling_forecasting(x))
        r = r.reset_index()
        r = r.rename(columns={'level_1':'timeForecasting'})

        # Add the forecasting horizon
        r['i'] = (r.time - r.timeForecasting)
        r['i'] = (r['i'] / np.timedelta64(1, 's')) / 3600
        r['i'] = r['i'].astype("int")
        r['i'] = r['i'].astype("str")

        # Pivot to a wide table, all info in one row
        meteo_vars = [i not in ["timeForecasting","time","i"] for i in list(r.columns)]
        rr = r.pivot_table(index="time", columns="i", values=list(r.columns[meteo_vars]))
        rr.columns = rr.columns.map('_'.join)

        # Add the location info
        rr['latitude'] = loc[1]
        rr['longitude'] = loc[2]
        rr['stationId'] = stationId
        rr['time'] = rr.index

        # Upload the data to Mongo
        rr_d = rr.to_dict('records')
        for i in xrange(len(rr_d)):
            mongo[params['forecasting']].update_many(
                {
                    "stationId": stationId,
                    "time": rr_d[i]["time"]
                }, {
                    "$set": rr_d[i]
                },
            upsert=True)
        print("{} items were uploaded to MongoDB".format(len(rr_d)))
        last_time = max(rr.index)
        mongo[params['mongodb']["stations_collection"]].update(
            {"stationId": stationId},
            {"$set":{"forecasting_time": last_time}},
            upsert=True
        )
    print("Closing MongoDB client")
    client.close()
