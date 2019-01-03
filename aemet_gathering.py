# -*- coding: utf-8 -*-
"""
    scripts to gather meteo_data from aemet using api
"""
import pytz
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import re
import pandas as pd
import os
import numpy as np

from utils import read_last_csv, remove_last_lines_csv, scrap_data

working_directory = os.getcwd()
working_directory = os.path.dirname(os.path.abspath(__file__))
data_file = "{wd}/meteo_data/{station}_hist_hourly.csv"
now = datetime.now()
today = datetime(now.year,now.month, now.day)

def get_meteo_data():
    headers = {
        "Accept": "application/javascript",
        "api_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJlZ2FiYWxkb25AY2ltbmUudXBjLmVkdSIsImp0aSI6IjJkNDcyYjUyLWVjODEtNDJjMi05ZGEwLTE1YjJlYzVjODIxYyIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTQ1MzE2ODM4LCJ1c2VySWQiOiIyZDQ3MmI1Mi1lYzgxLTQyYzItOWRhMC0xNWIyZWM1YzgyMWMiLCJyb2xlIjoiIn0.zuI75dPio4FnK_V0S1UlTd_QIxSdSYYf_arkhtnMaIo"
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

df = get_meteo_data()

data_by_station = df.groupby('idema')

for stationId, data in data_by_station:
    columns = {
        "time": 'fint',
        "temperature": 'ta',
        "humidity":'hr',
        "precipAccumulation": "prec",
        "windSpeed": "vv",
        "windBearing": "dv",
        "pressure": "pres",
        "stationId": "idema",
        "longitude": "lon",
        "latitude": "lat",
        "GHI": ""
    }
    final_dataframe = pd.DataFrame()
    for key, value in columns.items():
        if value not in data:
            final_dataframe[key] = [np.NaN]*len(data.index)
        else:
            final_dataframe[key] = data[value]
    final_dataframe['stationId'] = [stationId]*len(data.index)
    final_dataframe.index = pd.to_datetime(final_dataframe['time'])
    final_dataframe.tz_localize(pytz.UTC)
    final_dataframe.sort_index()


    # read file of historical data
    try:
        hist = read_last_csv(data_file.format(wd=working_directory, station=stationId), 48)
        hist = hist.set_index('time')
        hist.index = pd.to_datetime(hist.index)
        hist = hist.tz_localize(pytz.UTC)
        hist = hist.sort_index()
        headers = False
    except:
        hist = pd.DataFrame()
        headers = True
    if not hist.empty:
        remove_last_lines_csv(data_file.format(wd=working_directory, station=stationId), len(hist.index))

    hist = hist.append(final_dataframe)
    hist = hist.sort_index()
    hist = hist[~hist.index.duplicated(keep='last')]
    hist = hist.resample("H").mean()

    hist.to_csv(data_file.format(wd=working_directory, station=stationId), mode='a', header=headers)
