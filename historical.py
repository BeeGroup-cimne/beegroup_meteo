# -*- coding: utf-8 -*-

# Library imports
from bee_meteo import historical_weather
import json
import pytz
from pymongo import MongoClient, DESCENDING
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os
import glob
import utils

working_directory = os.path.dirname(os.path.abspath(__file__))

for config in glob.glob('{}/available_config/*.json'.format(working_directory)):
    with open(config) as f:
        params = json.load(f)
    # Check if the forecasting is needed for this configuration
    if not 'historical' in params:
        exit(0)
    locations = utils.read_locations(params)
    # Mongo connection
    client = MongoClient(params['mongodb']['host'], int(params['mongodb']['port']))
    client[params['mongodb']['db']].authenticate(
        params['mongodb']['username'],
        params['mongodb']['password']
    )
    mongo = client[params['mongodb']['db']]


    # Download the data and upload it to Mongo
    for loc in locations:

        stationId = "{:03.2f},{:03.2f}".format(loc[0],loc[1])
        print("Weather forecasting data for stationId {}".format(stationId))
        # Define the ts_from and ts_to
        try:
            cursor_ts = mongo[params['historical']].find({"stationId": stationId}).sort([("time",DESCENDING)])
            ts_from = pytz.UTC.localize(cursor_ts[0]["time"])
            ts_from -= relativedelta(hours=48)
        except:
            ts_from = pytz.UTC.localize(datetime.utcnow()-relativedelta(years=2))
            ts_to = pytz.UTC.localize(datetime.utcnow())

        # Download the historical weather data
        r = historical_weather(params['keys']['darksky'], params['keys']['CAMS'], loc[0], loc[1], ts_from, ts_to, csv_export=False)['hourly']

        # Add the location info
        r['latitude'] = loc[0]
        r['longitude'] = loc[1]
        r['stationId'] = stationId

        # Upload the data to Mongo
        r_d = r.to_dict('records')
        if mongo[params['mongodb']['historical']].find_one({"stationId": stationId}) is None:
            mongo[params['mongodb']['historical']].insert_many(r_d)
        else:
            for i in xrange(len(r_d)):
                mongo[params['mongodb']['historical']].update_many(
                    {
                        "stationId": stationId,
                        "time": r_d[i]["time"]
                    }, {
                        "$set": r_d[i]
                    },
                upsert=True)
        print("{} items were uploaded to MongoDB".format(len(r_d)))

    print("Closing MongoDB client")
    client.close()
