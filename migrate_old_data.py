from datetime import datetime
import requests
import pandas as pd
import numpy as np
import os
import glob

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



working_directory = os.getcwd()
save_file = "{wd}/meteo_data_check/{stationId}_hist_hourly.csv"

columns = {0: 'stationId', 1: 'time', 2: 'windSpeed', 3: 'windBearing', 6: 'temperature',
           7: 'humidity', 8: 'GHI', 9: 'pressure', 10: 'precipAccumulation'}
# get stations information

station_df = get_meteo_data()

for x in glob.glob("{}/migrate_data/*.met".format(working_directory)):
    df = pd.read_csv(x, header=None, names=range(0,12))
    df_f = pd.DataFrame()
    for key, value in columns.items():
        df_f[value] = df[key]
    station= df_f['stationId'][0]
    this_station = station_df[station_df.idema == station]
    if not this_station.empty:
    	lat = this_station.iloc[0].lat
    	lon = this_station.iloc[0].lon
    else:
	lat = None
	lon = None
    df_f['latitude'] = [lat] * len(df_f.index)
    df_f['longitude'] = [lon] * len(df_f.index)
    df_f['time'] = df_f['time'].astype(np.int64).apply(lambda x: datetime.strptime(str(x), "%Y%m%d%H%M"))
    df_f = df_f.set_index('time')
    df_f = df_f.sort_index()
    df_f.to_csv(save_file.format(wd=working_directory, stationId=station))
