# -*- coding: utf-8 -*-
import argparse
import logging
from datetime import datetime
import pandas as pd
import forecastio
import pytz
import json
import os
import utils
import numpy as np

def forecast_weather(api_key, lat, lon, units="si"):
    log = logging.getLogger(__name__)
    # Initialize variables
    hourly = []
    # Iterate through all needed days to download data day by day
    log.debug('### Downloading forecasted weather measures for latitude %s, longitude %s ###' % (lat, lon))

    meteo_data = forecastio.load_forecast(api_key, lat, lon, units=units)
    utc_timezone = pytz.UTC
    timezone = pytz.UTC #Remember! The output of this function will be always in UTC.

    # hourly dict
    for item in meteo_data.hourly().data:
        d = item.d
        d.update({
            'time': utc_timezone.localize(item.time).astimezone(timezone)
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
    log = logging.getLogger("forecasting")
    log.debug("starting the aemet gathering script")
    working_directory = os.getcwd()
    working_directory = os.path.dirname(os.path.abspath(__file__))
    log.debug("working directory is: {}".format(working_directory))
    with open('{}/general_config.json'.format(working_directory)) as f:
        config = json.load(f)
    log.debug("readed general config file")
    log.debug(config)

    data_directory = config['data_directory']
    data_file = "{wd}/{station}_forecast_hourly.csv"

    try:
        locations = utils.read_locations(config['forecast_stations'])
    except Exception as e:
        log.debug("Unable to load locations for config {}: {}".format(config, e))
        raise ValueError("Unable to load locations for config {}: {}".format(config, e))

    for loc in locations:
        stationId = loc['stationId']
        lat = loc['lat']
        lon = loc['lon']
        solar_radiation = loc['solar_radiation'] if 'solar_radiation' in loc else False
        if not stationId:
            stationId = "{:.2}_{:.2}".format(lat, lon)

        df_hourly = pd.DataFrame.from_records(forecast_weather(config['darksky']['apikey'], lat, lon, units="si"))
        df_hourly = df_hourly.set_index('time')
        df_hourly = df_hourly.sort_index()
        df_hourly = df_hourly.drop_duplicates(keep="last")
        pad_list = [x for x in ['summary', 'icon', 'precipType'] if x in df_hourly]
        df_hourly = df_hourly.resample('1H').mean().interpolate(limit=6).join(
            df_hourly.resample('1H')[pad_list].pad())

        if solar_radiation:
            df_hourly['time'] = df_hourly.index
            solar_data = utils.MG_solar_forecast(df_hourly, lat, lon)
            df_hourly = df_hourly.set_index('time')
            if solar_data is not None:
                solar_data = solar_data.set_index('time')
                solar_data = solar_data.resample('1H').mean().interpolate(limit=6)
                df_hourly = df_hourly.join(solar_data)
            df_hourly = df_hourly.reset_index()
        # now is the time of forecast
        now = min(df_hourly.time)

        df_hourly['timeForecasting'] = now
        df_hourly['lat'] = lat
        df_hourly['lon'] = lon
        df_hourly['stationId'] = stationId
        columns = ['timeForecasting']+config['meteo_header'] if not solar_radiation else ['timeForecasting']+config['meteo_header'] + config[
            'solar_forecast_header']
        for x in columns:
            if x not in df_hourly.columns:
                df_hourly[x] = np.nan

        df_hourly = df_hourly[columns]
        try:
            hist = utils.read_last_csv(data_file.format(wd=data_directory, station=stationId), 1)
            hist.timeForecasting = pd.to_datetime(hist.timeForecasting, utc=True)
            headers = False
        except FileNotFoundError as e:
            hist = pd.DataFrame()
            headers = True

        if hist.empty or hist.timeForecasting[0] != df_hourly.timeForecasting[0]:
            df_hourly.to_csv(data_file.format(wd=data_directory, station=stationId), mode='a', header=headers, index=False)


