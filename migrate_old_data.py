import re
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import os
import glob

from bs4 import BeautifulSoup

from utils import scrap_data


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
            [unicode.replace, "arg", ",", "."],
            [float, "arg"]
        ]),
        ('longitude', 'td', 4,[
            [unicode.replace, "arg", ",", "."],
            [float, "arg"]
        ]),
        ('status', 'td', 8, [

        ])
    ]
    return scrap_data(columns, rows[1:])



working_directory = os.getcwd()
save_file = "{wd}/meteo_data_check/{stationId}_hist_hourly.csv"

columns = {0: 'stationId', 1: 'time', 2: 'windSpeed', 3: 'windBearing', 6: 'temperature',
           7: 'humidity', 8: 'GHI', 9: 'pressure', 10: 'precipAccumulation'}
# get stations information

station_aemet_df = get_meteo_data()
station_meteocat_df = scrap_stations()

for x in glob.glob("{}/migrate_data/*.met".format(working_directory)):
    df = pd.read_csv(x, header=None, names=range(0,12))
    df_f = pd.DataFrame()
    for key, value in columns.items():
        df_f[value] = df[key]
    station= df_f['stationId'][0]
    this_station = station_aemet_df[station_aemet_df.idema == station]
    if not this_station.empty:
        lat = this_station.iloc[0].lat
        lon = this_station.iloc[0].lon
    else:
        this_station = station_meteocat_df[station_meteocat_df.stationId == station]
        if not this_station.empty:
            lat = this_station.iloc[0].latitude
            lon = this_station.iloc[0].longitude
        else:
            lat = None
            lon = None
    df_f['latitude'] = [lat] * len(df_f.index)
    df_f['longitude'] = [lon] * len(df_f.index)
    df_f['time'] = df_f['time'].astype(np.int64).apply(lambda x: datetime.strptime(str(x), "%Y%m%d%H%M"))
    df_f = df_f.set_index('time')
    df_f = df_f.sort_index()
    df_f.to_csv(save_file.format(wd=working_directory, stationId=station))
