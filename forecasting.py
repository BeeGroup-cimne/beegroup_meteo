#!/opt/BeeDataBackend/meteo/venv/bin/python

# Library imports
from bee_meteo import forecast_weather
import json
import pymongo
from pymongo.mongo_client import MongoClient

# Read params
with open('/opt/BeeDataBackend/meteo/config.json') as f:
    params = json.load(f)

# Create the location list to download
locations = [[float(j) for j in i.split(",")] for i in params['locations']]

# Download the data and upload it to Mongo
for loc in locations:
    
    # Download the meteo forecastings
    r = forecast_weather(params['keys']['darksky'], loc[0], loc[1], csv_export=True,
                        wd='/opt/BeeDataBackend/meteo')
