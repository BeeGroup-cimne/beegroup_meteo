# -*- coding: utf-8 -*-
import argparse
import json
import glob
import logging
import os
# Read params
from datetime import datetime

import dateutil
import pandas as pd
import pytz
from pymongo import MongoClient

import utils


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", nargs=1, default=["CRITICAL"], help="set the log level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET)")
    args = parser.parse_args()
    if args.level[0] in ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]:
        logging.basicConfig(level=args.level[0])
    else:
        raise ValueError("The log level {} does not exist".format(args.level[0]))
    log = logging.getLogger("forecasting")
    log.debug("starting the forecasting script")
    working_directory = os.getcwd()
    working_directory = os.path.dirname(os.path.abspath(__file__))
    log.debug("working directory is: {}".format(working_directory))
    with open('{}/general_config.json'.format(working_directory)) as f:
        config = json.load(f)
    log.debug("readed general config file")
    log.debug(config)

    data_directory = config['data_directory']
    data_file = "{wd}/{station}_forecast_hourly.csv"

    for config in glob.glob('{}/available_config/*.json'.format(working_directory)):
        with open(config) as f:
            params = json.load(f)
        # Check if the forecasting is needed for this configuration
        if not 'forecasting' in params:
            exit(0)
        try:
            locations = utils.read_locations(params['forecasting'])
        except Exception as e:
            print("Unable to load locations for config {}: {}".format(config, e))
            continue

            # Mongo connection
        log.debug("connecting to mongo")
        client = MongoClient(params['mongodb']['host'], int(params['mongodb']['port']))
        client[params['mongodb']['db']].authenticate(
            params['mongodb']['username'],
            params['mongodb']['password']
        )
        mongo = client[params['mongodb']['db']]


        # Download the data and upload it to Mongo
        for loc in locations:
            stationId = loc['stationId']
            lat = loc['lat']
            lon = loc['lon']
            if not stationId:
                stationId = "{:.2}_{:.2}".format(lat, lon)

            if not os.path.isfile(data_file.format(wd=data_directory, station=stationId)):
                continue

            #search for the last timestamp uploaded or the general requested timestamp
            try:
                station_info = mongo[params['forecasting']["stations_collection"]].find_one({"stationId": stationId}, sort=[('time', 1)])
                ts_from = pytz.UTC.localize(station_info["time"])
                hist = utils.read_last_csv(data_file.format(wd=data_directory, station=stationId), 500)
                hist = hist.set_index('time')
                hist.index = pd.to_datetime(hist.index, utc=True)
                if min(hist.index) <= ts_from:
                    hist = hist[hist.index >= ts_from]
                else:
                    raise Exception()
            except:
                ts_from = dateutil.parser.parse(params['forecasting']["timestamp_from"])
                hist = pd.read_csv(data_file.format(wd=data_directory, station=stationId))
                hist = hist.set_index('time')
                hist.index = pd.to_datetime(hist.index, utc=True, errors='coerce')
                hist = hist[hist.index >= ts_from]

            if not hist.empty:
                hist = hist.reset_index()
                mongo[params['forecasting']['mongo_collection']].insert_many(hist.to_dict(orient='records'))