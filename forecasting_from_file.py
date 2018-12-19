#!/opt/BeeDataBackend/meteo/venv/bin/python

# Library imports
import json
import pymongo
import pandas as pd
import numpy as np
import pytz
from pymongo import MongoClient, DESCENDING
from dateutil.relativedelta import relativedelta

def resampling_forecasting(x):
	try:
		time_x = x.name
		x = x.drop_duplicates("timeForecasting")
		x = x.set_index("timeForecasting")
		x = x.reindex(pd.DatetimeIndex(start=time_x-relativedelta(hours=48),end=time_x, freq="H"))
		x = x.fillna(method="bfill")
		x = x.resample("H").mean().interpolate()
	except:
		print ("{} Failed when resampling!".format(x.name))
	return (x)


# Read params
with open('/opt/BeeDataBackend/meteo/config.json') as f:
    params = json.load(f)


# Mongo connection
db = MongoClient(params['mongodb']['host'],int(params['mongodb']['port']))
db[params['mongodb']['db']].authenticate(
    params['mongodb']['username'],
    params['mongodb']['password']
)
ddbb = db[params['mongodb']['db']]


# Create the location list to download
locations = [[float(j) for j in i.split(",")] for i in params['locations']]


# Download the data and upload it to Mongo
for loc in locations:
    
    stationId = "{:03.2f},{:03.2f}".format(loc[0],loc[1])
    print("Weather forecasting data for stationId {}".format(stationId))
    
    try:
    	cursor_ts = ddbb[params['mongodb']['forecasting']].find({"stationId": stationId}).sort([("time",DESCENDING)])
    	ts = pytz.UTC.localize(cursor_ts[0]["time"])
    	ts -= relativedelta(hours=96)
    except:
    	ts = None
    
    # Read the meteo forecastings
    r = pd.read_csv("/opt/BeeDataBackend/meteo/meteo_data/{:.2f}_{:.2f}_forecasting_hourly.csv".format(loc[0],loc[1]))
    
    # Rearrange the time columns
    r.time = pd.to_datetime(r.time).dt.tz_localize(pytz.UTC)
    r.timeForecasting = pd.to_datetime(r.timeForecasting).dt.tz_localize(pytz.UTC)
    
    # Only get the last
    if ts: 
    	r = r[r.time >= ts]
    
    # Resample the timeForecasting to hourly.
    r = r.groupby(["time"]).apply(lambda x: resampling_forecasting(x))
    r = r.reset_index()
    r = r.rename(columns={'level_1':'timeForecasting'})
    
    # Add the forecasting horizon
    r['i'] = (r.time - r.timeForecasting)
    r['i'] = (r['i'] / np.timedelta64(1, 's')) / 3600
    r['i'] = r['i'].astype("int")
    r['i'] = r['i'].astype("str")
    
    # Pivot to a wide table, all info in one row
    meteo_vars = [i not in ["timeForecasting","time","i"] for i in list(r.columns)]
    rr = r.pivot_table(index="time", columns="i", values=list(r.columns[meteo_vars]))
    rr.columns = rr.columns.map('_'.join)
    
    # Add the location info
    rr['latitude'] = loc[0]
    rr['longitude'] = loc[1]
    rr['stationId'] = stationId
    rr['time'] = rr.index
    
    # Upload the data to Mongo
    rr_d = rr.to_dict('records')
    for i in xrange(len(rr_d)):
		ddbb[params['mongodb']['forecasting']].update_many(
			{
				"stationId": stationId,
				"time": rr_d[i]["time"]
			}, {
				"$set": rr_d[i]
			},
		upsert=True)
    print("{} items were uploaded to MongoDB".format(len(rr_d)))

print("Closing MongoDB client")
db.close()
