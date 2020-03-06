# -*- coding: utf-8 -*-
"""
    scripts to gather meteo_data from darksky
"""
import copy

import forecastio
import json
import logging
import os
import argparse
from datetime import datetime
import pytz
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from six import StringIO

import utils
import numpy as np
import multiprocessing.pool as mp


def get_meteo_data(lat, lon, time, now, apikey):
    log.debug("Downloading data for {}".format(time))
    meteo_data = forecastio.load_forecast(apikey, lat, lon, time=time, units="si")
    hourly = []
    for item in meteo_data.hourly().data:
        d = item.d
        d.update({
            'time': pytz.UTC.localize(item.time)
        })
        hourly.append(d)

    return hourly


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", nargs=1, default=["CRITICAL"], help="set the log level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET)")
    args = parser.parse_args()
    if args.level[0] in ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]:
        logging.basicConfig(level=args.level[0])
    else:
        raise ValueError("The log level {} does not exist".format(args.level[0]))
    log = logging.getLogger("darksky")
    log.debug("starting the darksky gathering script")
    working_directory = os.getcwd()
    working_directory = os.path.dirname(os.path.abspath(__file__))
    log.debug("working directory is: {}".format(working_directory))
    with open('{}/general_config.json'.format(working_directory)) as f:
        config = json.load(f)
    log.debug("readed general config file")
    log.debug(config)

    try:
        locations = utils.read_locations(config['darksky']['stations'])
    except Exception as e:
        log.debug("Unable to load locations for config {}: {}".format(config, e))
        raise ValueError("Unable to load locations for config {}: {}".format(config, e))

    data_directory = config['data_directory']
    data_file = "{wd}/{station}_hist_hourly.csv"

    now = datetime.utcnow()
    now = pytz.UTC.localize(now)
    log.info("darksky importing for {}".format(now))
    ts_from = None
    for loc in locations:
        stationId = loc['stationId']
        lat = loc['lat']
        lon = loc['lon']
        solar_radiation = loc['solar_radiation'] if 'solar_radiation' in loc else False
        log.debug("Importing {}".format(loc))

        if not stationId:
            stationId = "{:.2}_{:.2}". format(lat, lon)

        try:
            hist = utils.read_last_csv(data_file.format(wd=data_directory, station=stationId), 720)
            hist.index = pd.to_datetime(hist['time'], utc=True)
            hist = hist.sort_index()
            headers = False
        except FileNotFoundError as e:
            hist = pd.DataFrame()
            headers = True

        if not hist.empty:
            utils.remove_last_lines_csv(data_file.format(wd=data_directory, station=stationId), len(hist.index))
            ts_from = min(hist.index)

        today = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo)

        if ts_from and ts_from != today:
            ts_day = datetime(ts_from.year, ts_from.month, ts_from.day, tzinfo=ts_from.tzinfo)

            date_range = list(pd.date_range(ts_day, today))
            date_range = date_range[:-1] + [now]
        else:
            date_range = [now]

        pool = mp.ThreadPool(processes=config['processes'])
        results = [pool.apply_async(get_meteo_data, args=(lat, lon, x, now, config['darksky']['apikey'])) for x in date_range]
        results = [p.get() for p in results]
        pool.close()

        df_hourly = pd.concat([pd.DataFrame.from_records(x) for x in results],sort=True)

        log.debug('Successful downloading process!')

        df_hourly.index = df_hourly.time
        df_hourly = df_hourly.sort_index()
        df_hourly = df_hourly.drop_duplicates(keep="last")
        if ts_from:
            df_hourly = df_hourly[(ts_from <= df_hourly.time) & (df_hourly.time <= now)]
        else:
            df_hourly = df_hourly[df_hourly.time <= now]

        df_hourly = df_hourly.resample('1H').mean().interpolate(limit=6).join(df_hourly.resample('1H')[['summary','icon','precipType']].pad())
        df_hourly['time'] = df_hourly.index

        if solar_radiation:
            hist_temp = hist.append(df_hourly, sort=False)
            hist_temp = hist_temp.sort_index()
            hist_temp = hist_temp[~hist_temp.index.duplicated(keep='first')]
            hist_temp['time'] = hist_temp.index
            solar_data = utils.get_solar_radiation(hist_temp, config, lat, lon)
            if solar_data is not None:
                solar_data = solar_data.set_index('time')
                solar_data = solar_data.resample('1H').mean().interpolate(limit=6)
                df_hourly = df_hourly.join(solar_data)

        hist = hist.append(df_hourly, sort=False)
        hist = hist.sort_index()
        hist = hist[~hist.index.duplicated(keep='first')]
        hist['time'] = hist.index
        hist['lat'] = lat
        hist['lon'] = lon
        hist['stationId'] = stationId

        columns = config['meteo_header'] if not solar_radiation else config['meteo_header'] + config['solar_historical_header']
        for x in columns:
            if x not in hist.columns:
                hist[x] = np.nan
        hist = hist[columns]
        hist.to_csv(data_file.format(wd=data_directory, station=stationId), mode='a', header=headers, index=False)








