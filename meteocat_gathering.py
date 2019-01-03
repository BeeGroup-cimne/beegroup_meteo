# -*- coding: utf-8 -*-
"""
    scripts to gather meteo_data from meteocat using web scrapping
"""
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import re
import pandas as pd
import os
import pytz

from utils import read_last_csv, remove_last_lines_csv, scrap_data

working_directory = os.getcwd()
working_directory = os.path.dirname(os.path.abspath(__file__))
data_file = "{wd}/meteo_data/{station}_hist_hourly.csv"
now = datetime.now()
timezone = "Europe/Madrid"
today = datetime(now.year,now.month, now.day, tzinfo=pytz.timezone(timezone))
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


def scrapp_meteo_for_date(date, codi, lat, long):
    try:
        url = 'http://www.meteo.cat/observacions/xema/dades?codi={}&dia={}Z'.format(codi, date.strftime("%Y-%m-%dT%H:%M"))
        print(url)
        r = requests.get(url)
        html = BeautifulSoup(r.text, 'html.parser')
        table = html.select('.tblperiode')[0]
        rows = table.select('tr')
        columns = [
                      ('time', 'th', 0, [
                          [unicode.strip, "arg"],
                          [unicode.split, "arg"],
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
        print("Error in {} {}:{}".format(codi, date, e))
        return {}


stations = pd.DataFrame.from_records(scrap_stations())
stations = stations[stations.status == u"Operativa"]

for s in list(stations.iterrows()):
    #read file of historical data
    try:
        hist = read_last_csv(data_file.format(wd=working_directory, station=s[1].stationId), 48)
        hist = hist.set_index('time')
        hist.index = pd.to_datetime(hist.index)
        hist = hist.tz_localize(pytz.UTC)
        hist = hist.sort_index()
    except:
        hist = pd.DataFrame()
    headers = True
    if not hist.empty:
        last_date = max(hist.index)
        last_date = datetime(last_date.year, last_date.month, last_date.day, tzinfo=pytz.UTC)
        hist = hist[hist.index >= last_date]
        remove_last_lines_csv(data_file.format(wd=working_directory, station=s[1].stationId), len(hist.index))
        headers = False
    else:
        last_date = today - timedelta(days=10)
    date_list = pd.date_range(last_date.astimezone(pytz.UTC),today.astimezone(pytz.UTC))

    for date in date_list:
        new_meteo = pd.DataFrame(scrapp_meteo_for_date(date, s[1].stationId, lat=s[1].latitude, long=s[1].longitude))
        if new_meteo.empty:
            continue
        new_meteo = new_meteo.set_index('time')
        new_meteo = new_meteo.sort_index()
        hist = hist.append(new_meteo)
        hist = hist.sort_index()
        hist = hist[~hist.index.duplicated(keep='first')]
        hist = hist.resample("H").mean()
    hist.to_csv(data_file.format(wd=working_directory, station=s[1].stationId), mode='a', header=headers)