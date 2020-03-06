# -*- coding: utf-8 -*-

# Library imports
import pandas as pd
import json
import pytz
from pymongo import MongoClient, DESCENDING
from dateutil.relativedelta import relativedelta
import dateutil
from datetime import datetime
import os
import glob
import utils
import logging
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", nargs=1, default=["CRITICAL"], help="set the log level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET)")
    args = parser.parse_args()
    if args.level[0] in ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]:
        logging.basicConfig(level=args.level[0])
    else:
        raise ValueError("The log level {} does not exist".format(args.level[0]))
    log = logging.getLogger("historical")
    log.debug("starting the meteo historic script")
    #working_directory = os.getcwd()
    working_directory = os.path.dirname(os.path.abspath(__file__))
    with open('{}/general_config.json'.format(working_directory)) as f:
        gconfig = json.load(f)
    data_directory = gconfig['data_directory']
    data_file = "{wd}/{station}_historical_hourly.csv"

    for config in glob.glob('{}/available_config/*.json'.format(working_directory)):
        with open(config) as f:
            params = json.load(f)
        # Check if the historic is needed for this configuration
        if not 'historical' in params:
            continue

        # Mongo connection
        log.debug("connecting to mongo")
        client = MongoClient(params['mongodb']['host'], int(params['mongodb']['port']))
        client[params['mongodb']['db']].authenticate(
            params['mongodb']['username'],
            params['mongodb']['password']
        )
        mongo = client[params['mongodb']['db']]

        try:
            locations = utils.read_locations(params['historical'], mongo)
        except Exception as e:
            log.error("Unable to load locations for config {}: {}".format(config, e))
            continue

        # Download the data and upload it to Mongo
        for loc in locations:
            stationId = loc['stationId']
            lat = loc['lat']
            lon = loc['lon']
            if not stationId:
                stationId = "{lat:.2f}_{lon:.2f}".format(lat=lat, lon=lon)

            if not os.path.isfile(data_file.format(wd=data_directory, station=stationId)):
                continue

            print("Weather forecasting data for stationId {}".format(stationId))

            # search for the last timestamp uploaded or the general requested timestamp
            try:
                station_info = mongo[params['historical']["stations_collection"]].find_one({"stationId": stationId},
                                                                                            sort=[('time', -1)])
                ts_from = pytz.UTC.localize(station_info["time"])
                hist = utils.read_last_csv(data_file.format(wd=data_directory, station=stationId), 500)
                hist = hist.set_index('time')
                hist.index = pd.to_datetime(hist.index, utc=True)
                if min(hist.index) <= ts_from:
                    hist = hist[hist.index > ts_from]
                else:
                    raise Exception()
                hist = hist.sort_index()
                hist = hist.loc[~hist.index.duplicated(keep='last')]
            except:
                ts_from = dateutil.parser.parse(params['historical']["timestamp_from"])
                hist = pd.read_csv(data_file.format(wd=data_directory, station=stationId))
                hist = hist.set_index('time')
                hist.index = pd.to_datetime(hist.index, utc=True, errors='coerce')
                hist = hist[hist.index >= ts_from]
                hist = hist.sort_index()
                hist = hist.loc[~hist.index.duplicated(keep='last')]

            if not hist.empty:
                hist = hist.reset_index()
                mongo[params['historical']['mongo_collection']].insert_many(hist.to_dict(orient='records'))
        log.debug("Closing MongoDB client")
        client.close()
