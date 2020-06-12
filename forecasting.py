# -*- coding: utf-8 -*-
import argparse
import json
import glob
import logging
import os
# Read params
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil
import pandas as pd
import pytz
from pymongo import MongoClient
import numpy as np
import utils


def resampling_forecasting(x):
    try:
        time_x = x.name
        x = x.drop_duplicates("timeForecasting")
        x = x.set_index("timeForecasting")
        x = x.reindex(pd.DatetimeIndex(start=time_x - relativedelta(hours=48), end=time_x, freq="H"))
        x = x.fillna(method="bfill")
        x = x.resample("H").mean().interpolate()
    except Exception as e:
        print("{} Failed when resampling! {}".format(x.name, e))
    return (x)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", nargs=1, default=["CRITICAL"], help="set the log level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET)")
    parser.add_argument("-ho", "--how", nargs=1, default=["online"], help="set the type of data: forecasted: each row will have the measure predicted for that timestep in the 48h previous. online: each row will have the 48h prediction of the forecasting timestep")
    args = parser.parse_args()
    if args.level[0] in ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]:
        logging.basicConfig(level=args.level[0])
    else:
        raise ValueError("The log level {} does not exist".format(args.level[0]))
    if args.how[0] in ["online", "forecasted"]:
        how = args.how[0]
    else:
        raise ValueError("The how parameter {} does is not suported".format(args.level[0]))
    log = logging.getLogger("forecasting")
    log.debug("starting the forecasting script")
    working_directory = os.getcwd()
    working_directory = os.path.dirname(os.path.abspath(__file__))
    log.debug("working directory is: {}".format(working_directory))
    with open('{}/general_config.json'.format(working_directory)) as f:
        gconfig = json.load(f)
    log.debug("readed general config file")
    log.debug(gconfig)

    data_directory = gconfig['data_directory']
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

            #search for the last timestamp uploaded in the last week or the general requested timestamp
            now = datetime.utcnow() - relativedelta(weeks=1)
            try:
                station_info = mongo[params['forecasting']["stations_collection"]].find_one({"stationId": stationId, "time":{"$gt": now}}, sort=[('time', -1)])
                ts_from = pytz.UTC.localize(station_info["time"])
                hist = utils.read_last_csv(data_file.format(wd=data_directory, station=stationId), 100*48)
                hist.timeForecasting = pd.to_datetime(hist.timeForecasting, utc=True)
                hist.time = pd.to_datetime(hist.time, utc=True)
                if min(hist.timeForecasting) <= ts_from:
                    hist = hist[hist.timeForecasting > ts_from]
                else:
                    raise Exception()
            except:
                ts_from = dateutil.parser.parse(params['forecasting']["timestamp_from"])
                hist = pd.read_csv(data_file.format(wd=data_directory, station=stationId), dtype={'icon': str, 'precipType': str, 'summary': str})
                hist.timeForecasting = pd.to_datetime(hist.timeForecasting, utc=True)
                hist.time = pd.to_datetime(hist.time, utc=True)
                hist = hist[hist.timeForecasting > ts_from]

            if not hist.empty:
                # Resample the timeForecasting to hourly.
                r = hist.groupby(["time"]).apply(lambda x: resampling_forecasting(x))
                r = r.reset_index()
                r = r.rename(columns={'level_1': 'timeForecasting'})
                # Add the forecasting horizon
                r['i'] = (r.time - r.timeForecasting)
                r['i'] = (r['i'] / np.timedelta64(1, 's')) / 3600
                r['i'] = r['i'].astype("int")
                r['i'] = r['i'].astype("str")
                # Pivot to a wide table, all info in one row
                meteo_vars = [i not in ["timeForecasting", "time", "i", "lat", "lon", "stationId"] for i in list(r.columns)]
                # if you consider "time" in the index argument, each horizon defines the measure predicted for that timestep,
                # so each row will have to contain similar values, as it is the forecasted version of the same timestep.
                # if you consider "timeForecasting" in the index argument, each horizon defines the 48h prediction of the forecasting timestep.
                if how == "online":
                    rr = r.pivot_table(index="timeForecasting", columns="i", values=list(r.columns[meteo_vars]))
                    rr.columns = rr.columns.map('_'.join)
                    # we filter future forecastings for the online
                    rr = rr[rr.index <= pytz.UTC.localize(datetime.utcnow())]
                else:
                    rr = r.pivot_table(index="time", columns="i", values=list(r.columns[meteo_vars]))
                    rr.columns = rr.columns.map('_'.join)
                rr['lat'] = lat
                rr['lon'] = lon
                rr['stationId'] = stationId
                rr['time'] = rr.index
                mongo[params['forecasting']['mongo_collection']].insert_many(rr.to_dict(orient='records'))
        log.debug("Closing MongoDB client")
        client.close()