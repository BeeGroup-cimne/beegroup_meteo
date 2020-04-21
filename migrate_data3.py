import glob
import pandas as pd
import json
import os
import numpy as np
import pytz
import requests

migrate_directory = ""
working_directory = os.getcwd()
working_directory = os.path.dirname(os.path.abspath(__file__))

with open('{}/general_config.json'.format(working_directory)) as f:
    config = json.load(f)

data_directory = config['data_directory']
fail_data_directory = config['data_directory'] + "failed/"
if not os.path.isdir(fail_data_directory):
    os.mkdir(fail_data_directory)


def get_meteo_data(apikey):
    headers = {"Accept": "application/javascript","api_key": apikey}
    url = "https://opendata.aemet.es/opendata/api/observacion/convencional/todas"
    r = requests.get(url, headers=headers)
    if r.ok:
        r = requests.get(r.json()['datos'])
        if r.ok:
            data = r.json()
            df = pd.DataFrame.from_records(data)
            return df
    raise Exception("Error in performing the request {}".format(r.text))


for file_name in glob.glob("{}/*.csv".format(migrate_directory)):
    df = pd.read_csv(file_name)
    # check if this station has location ("required parameter in the file")
    file_name_final = file_name.split("/")[-1]
    if not all(c in df.columns for c in ['latitude','longitude']):
        try:
            lat = float(file_name_final.split("_")[0])
            lon = float(file_name_final.split("_")[1])
        except:
            df.to_csv("{}/{}".format(fail_data_directory, file_name_final))
            continue
    else:
        lat = float(df.latitude.tolist()[0])
        lon = float(df.longitude.tolist()[0])
    if not "stationId" in df.columns:
        stationId = file_name_final.split("_hist_hourly.csv")[0]
    else:
        stationId = df.stationId.tolist()[0]
    df.index = pd.to_datetime(df['time'])
    df = df.sort_index()
    try:
        df.index = df.index.tz_localize(pytz.UTC)
    except:
        pass
    df['time'] = df.index
    df['lat'] = lat
    df['lon'] = lon
    df['stationId'] = stationId
    if "GHI" in df.columns:
        file_headers = config['meteo_header'] + config['solar_historical_header']
    else:
        file_headers = config['meteo_header']
    for x in file_headers:
        if x not in df.columns:
            df[x] = np.nan
    df = df[file_headers]
    df.to_csv("{}/{}".format(data_directory, file_name_final), header=file_headers, index=None)

df = get_meteo_data(config['aemet']['apikey'])
data_by_station = df.groupby('idema')

for file_name in glob.glob("{}/*.csv".format(fail_data_directory)):
    file_name_final = file_name.split("/")[-1]
    stationId = file_name_final.split("_hist_hourly.csv")[0]
    data = data_by_station.get_group(stationId)
    lat = data.lat.tolist()[0]
    lon = data.lon.tolist()[0]
    df = pd.read_csv(file_name)
    df.index = pd.to_datetime(df['time'])
    df = df.sort_index()
    try:
        df.index = df.index.tz_localize(pytz.UTC)
    except:
        pass
    df['time'] = df.index
    df['lat'] = lat
    df['lon'] = lon
    df['stationId'] = stationId
    if "GHI" in df.columns:
        file_headers = config['meteo_header'] + config['solar_historical_header']
    else:
        file_headers = config['meteo_header']
    for x in file_headers:
        if x not in df.columns:
            df[x] = np.nan
    df = df[file_headers]
    df.to_csv("{}/{}".format(data_directory, file_name_final), header=file_headers, index=None)
    os.remove(file_name)
