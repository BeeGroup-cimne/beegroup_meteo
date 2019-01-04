# -*- coding: utf-8 -*-

# Library imports
import pandas as pd
from bee_meteo import historical_weather
import json
import pytz
from pymongo import MongoClient, DESCENDING
from dateutil.relativedelta import relativedelta
import dateutil
from datetime import datetime
import os
import glob
import utils

#working_directory = os.getcwd()
working_directory = os.path.dirname(os.path.abspath(__file__))

for config in glob.glob('{}/available_config/*.json'.format(working_directory)):
    with open(config) as f:
        params = json.load(f)
    # Check if the forecasting is needed for this configuration
    if not 'historical' in params:
        exit(0)

    # Mongo connection
    client = MongoClient(params['mongodb']['host'], int(params['mongodb']['port']))
    client[params['mongodb']['db']].authenticate(
        params['mongodb']['username'],
        params['mongodb']['password']
    )
    mongo = client[params['mongodb']['db']]

    try:
        locations = utils.read_locations(params['historical'], mongo)
    except Exception as e:
        print("Unable to load locations for config {}: {}".format(config, e))
        continue



    # Download the data and upload it to Mongo
    for stationId, latitude, longitude in locations:

        if not stationId:
            stationId = "{lat:.2f}_{lon:.2f}".format(lat=latitude, lon=longitude)
        data_file = "{}/meteo_data/{}_hist_hourly.csv".format(working_directory, stationId)
        print("Weather forecasting data for stationId {}".format(stationId))
        # Define the ts_from and ts_to
        try:
            station_info = mongo[params['mongodb']["stations_collection"]].find_one({"stationId": stationId})
            ts_from = pytz.UTC.localize(station_info["historic_time"])
            ts_from -= relativedelta(hours=48)
        except:
            ts_from = dateutil.parser.parse(params['historical']["timestamp_from_first_upload"])
        ts_to = pytz.UTC.localize(datetime.utcnow())


        if os.path.isfile(data_file):
            meteo_df = pd.read_csv(data_file)
            meteo_df = meteo_df.set_index('time')
            meteo_df.index = pd.to_datetime(meteo_df.index)
            meteo_df['time'] = meteo_df.index
            meteo_df = meteo_df.tz_localize(pytz.UTC)
            meteo_df = meteo_df.sort_index()
        else:
            meteo_df = None

        if 'gather_last_time' in params['historical'] and not params['historical']['gather_last_time']:
            time_offset = relativedelta(hours=24)
        else:
            time_offset=relativedelta(hours=0)

        if not meteo_df is None and ts_from >= min(meteo_df.index) and ts_to - time_offset <= max(meteo_df.index):
            r = meteo_df[ts_from:ts_to]
        elif 'keys' in params and latitude and longitude:
            # Download the historical weather data
            r = historical_weather(params['keys']['darksky'], params['keys']['CAMS'], latitude, longitude, ts_from, ts_to,
                                   csv_export=True, wd=working_directory, stationId=stationId)['hourly']
        elif meteo_df is not None:
            r = meteo_df[ts_from:ts_to]
        else:
            print("No data could be found by station {}".format(stationId))
            continue
        # Add the location info if it comes from location
        if latitude:
            r['latitude'] = latitude
        if longitude:
            r['longitude'] = longitude
        if stationId:
            r['stationId'] = stationId

        # Upload the data to Mongo
        r_d = r.to_dict('records')
        if not r_d:
            print("No data could be found by station {}".format(stationId))
            continue
        if mongo[params['historical']["mongo_collection"]].find_one({"stationId": stationId}) is None:
            mongo[params['historical']["mongo_collection"]].insert_many(r_d)
        else:
            for i in xrange(len(r_d)):
                mongo[params['historical']["mongo_collection"]].update_many(
                    {
                        "stationId": stationId,
                        "time": r_d[i]["time"]
                    }, {
                        "$set": r_d[i]
                    },
                upsert=True)
        print("{} items were uploaded to MongoDB".format(len(r_d)))
        #save last time to the mongo_collection
        last_time = max(r.index)
        mongo[params['mongodb']["stations_collection"]].update(
            {"stationId": stationId},
            {"$set":{"historic_time": last_time}},
            upsert=True
        )
    print("Closing MongoDB client")
    client.close()
