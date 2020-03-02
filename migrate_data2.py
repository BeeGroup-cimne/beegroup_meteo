import glob
import pandas as pd
import json
import os
import numpy as np
import pytz

migrate_directory = "/opt/beegroup_meteov2/migrate_data"
working_directory = os.getcwd()
working_directory = os.path.dirname(os.path.abspath(__file__))

with open('{}/general_config.json'.format(working_directory)) as f:
    config = json.load(f)

data_directory = config['data_directory']
for x in glob.glob("{}/*.csv".format(migrate_directory)):
    df = pd.read_csv(x)
    lat = x.split("/")[-1].split("_")[0]
    lon = x.split("/")[-1].split("_")[1]
    stationId = "{}_{}".format(lat,lon)
    df.time = pd.to_datetime(df.time)
    df.timeForecasting = pd.to_datetime(df.timeForecasting)
    df['horizon'] = (df.time - df.timeForecasting) / np.timedelta64(1, 's') / 3600
    df.horizon = df.horizon.astype("int").astype("str")
    meteo_vars = [i not in ["time", "timeForecasting" "horizon"] for i in list(df.columns)]
    df.time = df.timeForecasting
    rr = df.pivot_table(index="time", columns="horizon", values=list(df.columns[meteo_vars]))
    rr.columns = rr.columns.map('_'.join)
    rr['time'] = rr.index.tz_localize(pytz.UTC)
    rr['lat'] = lat
    rr['lon'] = lon
    rr['stationId'] = stationId
    headers = config['meteo_header'] + config['solar_forecast_header']
    for y in headers:
        if y not in ['time', 'lat', 'lon', 'stationId']:
            for c in ["{}_{}".format(y, i) for i in range(0, 49)]:
                if c not in rr.columns:
                    rr[c] = np.nan
        else:
            if y not in rr.columns:
                rr[c] = np.nan
    data_file = "{wd}/{station}_forecast_hourly.csv"
    rr.to_csv(data_file.format(wd=data_directory, station=stationId), mode='a', header=True, index=False)
