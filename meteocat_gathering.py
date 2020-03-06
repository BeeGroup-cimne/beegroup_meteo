# -*- coding: utf-8 -*-
"""
    scripts to gather meteo_data from meteocat using web scrapping
"""
import argparse
import json
import logging

import multiprocessing.pool as mp
import pytz
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import re
import pandas as pd
import os
import numpy as np
from utils import read_last_csv, remove_last_lines_csv, scrap_data, get_solar_radiation, read_locations

def scrap_stations():
    url = 'http://www.meteo.cat/observacions/llistat-xema'
    r = requests.get(url)
    html = BeautifulSoup(r.text, 'html.parser')
    table = html.select('#llistaEstacions')[0]
    rows = table.select('tr')
    columns = [
        ('stationId', 'td', 2, [
            [re.findall, "\[(.*?)\]", "arg"],
            [list.__getitem__, "arg", 0]
        ]),
        ('latitude', 'td', 3,[
            [str.replace, "arg", ",", "."],
            [float, "arg"]
        ]),
        ('longitude', 'td', 4,[
            [str.replace, "arg", ",", "."],
            [float, "arg"]
        ]),
        ('status', 'td', 8, [

        ])
    ]
    return scrap_data(columns, rows[1:])


def scrapp_meteo_for_date(date, codi, lat, long):
    try:
        url = 'http://www.meteo.cat/observacions/xema/dades?codi={}&dia={}Z'.format(codi, date.strftime("%Y-%m-%dT%H:%M"))
        log.debug("obtaining data from {}".format(url))
        r = requests.get(url)
        html = BeautifulSoup(r.text, 'html.parser')
        table = html.select('.tblperiode')[0]
        rows = table.select('tr')
        columns = [
                      ('time', 'th', 0, [
                          [str.strip, "arg"],
                          [str.split, "arg"],
                          [list.__getitem__,"arg", 0],
                          [datetime.strptime,"arg","%H:%M"],
                          [datetime.time, "arg"],
                          [datetime.combine, date.date(), "arg"],
                          [pytz.UTC.localize, "arg"]
                        ]

                       ),
                      ('temperature', 'td', 0, [[float, "arg"]]),
                      ('humidity', 'td', 3, [[float, "arg"]]),
                      ('precipAccumulation', 'td', 4, [[float, "arg"]]),
                      ('windSpeed', 'td', 5, [[float, "arg"]]),
                      ('windBearing','td',6, [[float, "arg"]]),
                      ('pressure', 'td', 8, [[float, "arg"]]),
                      ('GHI', 'td', 9, [[float, "arg"]])
                    ]
        return scrap_data(columns, rows[1:], stationId=codi, latitude=lat, longitude=long)
    except Exception as e:
        log.critical("Error in {} {}:{}".format(codi, date, e))
        return {}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--level", nargs=1, default=["CRITICAL"], help="set the log level (CRITICAL/ERROR/WARNING/INFO/DEBUG/NOTSET)")
    args = parser.parse_args()
    if args.level[0] in ["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]:
        logging.basicConfig(level=args.level[0])
    else:
        raise ValueError("The log level {} does not exist".format(args.level[0]))
    log = logging.getLogger("meteocat")
    log.debug("starting the meteocat gathering script")
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
    timezone = "Europe/Madrid"
    today = datetime(now.year,now.month, now.day)

    stations = pd.DataFrame.from_records(scrap_stations())
    stations = stations[stations.status == u"Operativa"]

    try:
        locations = read_locations(config['meteocat'])
    except Exception as e:
        log.debug("Unable to load locations for config {}: {}, no solar stations".format(config, e))
        locations = None

    for s in list(stations.iterrows()):
        log.debug("obtaining data from station {}".format(s[1].stationId))
        solar_radiation = False
        if locations:
            loc = [x for x in locations if x['stationId'] == s[1].stationId]
            if loc:
                solar_radiation = loc[0]['solar_radiation'] if 'solar_radiation' in loc[0] else False
        if solar_radiation:
            historical_rewrite = 720
        else:
            historical_rewrite = 48
        try:
            hist = read_last_csv(data_file.format(wd=data_directory, station=s[1].stationId), historical_rewrite)
            hist.index = pd.to_datetime(hist['time'])
            hist = hist.sort_index()
            headers = False
        except:
            hist = pd.DataFrame()
            headers = True
        if not hist.empty:
            remove_last_lines_csv(data_file.format(wd=data_directory, station=s[1].stationId), len(hist.index))
            last_date = max(hist.index)
            last_date = datetime(last_date.year, last_date.month, last_date.day)
        else:
            # TODO: set the minimum historical to some other value
            last_date = today - timedelta(days=365)

        date_list = pd.date_range(last_date,today)

        pool = mp.ThreadPool(processes=config['processes'])
        results = [pool.apply_async(scrapp_meteo_for_date, args=(x, s[1].stationId, s[1].latitude, s[1].longitude)) for x in date_list]
        results = [p.get() for p in results]
        pool.close()

        df_hourly = pd.concat([pd.DataFrame.from_records(x) for x in results], sort=True)

        log.debug('Successful downloading process!')

        df_hourly.index = df_hourly.time
        df_hourly = df_hourly.sort_index()
        df_hourly = df_hourly.drop_duplicates(keep="last")
        df_hourly = df_hourly.resample('1H').mean().interpolate(limit=6).join(
            df_hourly.resample('1H').stationId.pad())
        df_hourly['time'] = df_hourly.index

        if solar_radiation:
            hist_temp = hist.append(df_hourly, sort=False)
            hist_temp = hist_temp.sort_index()
            hist_temp = hist_temp[~hist_temp.index.duplicated(keep='first')]
            hist_temp['time'] = hist_temp.index
            solar_data = get_solar_radiation(hist_temp, config, s[1].latitude, s[1].longitude)
            if solar_data is not None:
                solar_data = solar_data.set_index('time')
                solar_data = solar_data.resample('1H').mean().interpolate(limit=6)
                df_hourly = df_hourly.join(solar_data)

        hist = hist.append(df_hourly, sort=False)
        hist = hist.sort_index()
        hist = hist[~hist.index.duplicated(keep='first')]
        hist['time'] = hist.index
        hist['lat'] = s[1].latitude
        hist['lon'] = s[1].longitude
        hist['stationId'] = s[1].stationId

        columns = config['meteo_header'] if not solar_radiation else config['meteo_header'] + config[
            'solar_historical_header']

        for x in columns:
            if x not in hist.columns:
                hist[x] = np.nan
        hist = hist[columns]
        hist.to_csv(data_file.format(wd=data_directory, station=s[1].stationId), mode='a', header=headers, index=None)
