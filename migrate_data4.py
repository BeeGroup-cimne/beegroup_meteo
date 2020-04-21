import glob
import pandas as pd
import json
import os
import numpy as np
import pytz
import requests

migrate_directory = "<set>"
data_directory = "<set>"
working_directory = os.getcwd()
working_directory = os.path.dirname(os.path.abspath(__file__))
with open('{}/general_config.json'.format(working_directory)) as f:
    config = json.load(f)
for file_name in glob.glob("{}/*.csv".format(migrate_directory)):
    file_name_final = file_name.split("/")[-1]
    df = pd.read_csv(file_name)
    columns = ["apparentTemperature", "cloudCover", "dewPoint", "humidity", "icon", "ozone",
               "precipAccumulation", "precipIntensity", "precipIntensityError", "precipProbability", "precipType", "pressure", "summary",
               "temperature", "uvIndex", "visibility", "windBearing", "windGust", "windSpeed", "GHI"]

    data = []
    for _, row in df.iterrows():
        try:
            timeForecasting = pd.to_datetime(row.time, utc=True)
        except:
            continue
        lat = row.lat
        lon = row.lon
        stationId = row.stationId
        for i in range(0, 49):
            time_row = timeForecasting + pd.DateOffset(hours=i)
            data_json = {"timeForecasting": timeForecasting, "time": time_row, "lat": lat, "lon": lon,
                         "stationId": stationId}
            for c in columns:
                data_json[c] = row["{}_{}".format(c, i)]
            data.append(data_json)
    df_n = pd.DataFrame.from_records(data=data, index=list(range(0, len(data))))
    final_columns = ["timeForecasting", "time", "apparentTemperature", "cloudCover", "dewPoint", "humidity", "icon",
                     "ozone", "precipAccumulation",
                     "precipIntensity", "precipIntensityError", "precipProbability", "precipType", "pressure",
                     "summary", "temperature", "uvIndex",
                     "visibility", "windBearing", "windGust", "windSpeed", "lat", "lon", "stationId", "GHI"]
    for x in final_columns:
        if x not in df_n.columns:
            df_n[x] = np.nan
    df_n = df_n[final_columns]
    df_n.to_csv("{}/{}".format(data_directory, file_name_final), header=True, index=False)