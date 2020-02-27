# -*- coding: utf-8 -*-
"""
    scripts to gather meteo_data from aemet using api
"""
import argparse
import json
import logging

import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import pytz
import numpy as np
from utils import read_last_csv, remove_last_lines_csv, scrap_data, read_locations, get_solar_radiation


def get_meteo_data(apikey):
    headers = {
        "Accept": "application/javascript",
        "api_key": apikey
    }
    url = "https://opendata.aemet.es/opendata/api/observacion/convencional/todas"
    r = requests.get(url, headers=headers)
    if r.ok:
        r = requests.get(r.json()['datos'])
        if r.ok:
            data = r.json()
            df = pd.DataFrame.from_records(data)
            return df
    raise Exception("Error in performing the request {}".format(r.text))


columns = [
    ("fint", "time"),
    ("prec", "precipAccumulation"),
    ("pacutp", "precipAccumulation"),
    ("vv", "windSpeed"),
    ("vvu", "windSpeed"),
    ("dv", "windBearing"),
    ("dvu", "windBearing"),
    ("hr", "humidity"),
    ("pres", "pressure"),
    ("ta", "temperature"),
    ("vis", "visibility"),
    ("lat", "lat"),
    ("lon", "lon"),
    ("idema", "stationId"),
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", nargs=1, default=["CRITICAL"], help="set the log level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET)")
    args = parser.parse_args()
    if args.level[0] in ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]:
        logging.basicConfig(level=args.level[0])
    else:
        raise ValueError("The log level {} does not exist".format(args.level[0]))
    log = logging.getLogger("aemet")
    log.debug("starting the aemet gathering script")
    working_directory = os.getcwd()
    working_directory = os.path.dirname(os.path.abspath(__file__))
    log.debug("working directory is: {}".format(working_directory))
    with open('{}/general_config.json'.format(working_directory)) as f:
        config = json.load(f)
    log.debug("readed general config file")
    log.debug(config)

    data_directory = config['data_directory']
    data_file = "{wd}/{station}_hist_hourly.csv"

    now = datetime.utcnow()
    now = pytz.UTC.localize(now)
    today = datetime(now.year,now.month, now.day)
    df = get_meteo_data(config['aemet']['apikey'])

    data_by_station = df.groupby('idema')

    try:
        locations = read_locations(config['aemet'])
    except Exception as e:
        log.debug("Unable to load locations for config {}: {}, no solar stations".format(config, e))
        locations = None

    for stationId, data in data_by_station:
        solar_radiation = False
        if locations:
            loc = [x for x in locations if x['stationId'] == stationId]
            if loc:
                solar_radiation = loc[0]['solar_radiation'] if 'solar_radiation' in loc[0] else False
        final_dataframe = pd.DataFrame()
        for key, value in columns:
            if key in data:
                final_dataframe[value] = data[key]
        final_dataframe.index = pd.to_datetime(final_dataframe['time'])
        final_dataframe.index = final_dataframe.index.tz_localize(pytz.UTC)
        final_dataframe = final_dataframe.sort_index()
        final_dataframe = final_dataframe.resample("H").mean().interpolate(limit=6).join(final_dataframe.resample("H").stationId.pad())

        lat = final_dataframe.lat[0]
        lon = final_dataframe.lon[0]
        final_dataframe['time'] = final_dataframe.index
        if solar_radiation:
            solar_data = get_solar_radiation(final_dataframe, config, lat, lon)
            if solar_data is not None:
                solar_data = solar_data.set_index('time')
                solar_data = solar_data.resample('1H').mean().interpolate(limit=6)
                df_hourly = final_dataframe.join(solar_data)

        # read file of historical data
        try:
            hist = read_last_csv(data_file.format(wd=data_directory, station=stationId), 48)
            hist.index = pd.to_datetime(hist['time'])
            hist = hist.sort_index()
            headers = False
        except:
            hist = pd.DataFrame()
            headers = True
        if not hist.empty:
            remove_last_lines_csv(data_file.format(wd=data_directory, station=stationId), len(hist.index))

        hist = hist.append(df_hourly, sort=False)
        hist = hist.sort_index()
        hist = hist[~hist.index.duplicated(keep='last')]
        hist['time'] = hist.index
        headers = config['historical_header'] if not solar_radiation else config['historical_header'] + config[
            'solar_header']

        for x in headers:
            if x not in hist.columns:
                hist[x] = np.nan
        hist = hist[headers]

        hist.to_csv(data_file.format(wd=data_directory, station=stationId), mode='a', header=headers, index=False)

