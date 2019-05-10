# -*- coding: utf-8 -*-
from bee_meteo import forecast_weather
import json
import glob
import os
# Read params
import utils

working_directory = os.path.dirname(os.path.abspath(__file__))
with open('{}/general_config.json'.format(working_directory)) as f:
    config = json.load(f)
data_directory = config['data_directory']

for config in glob.glob('{}/available_config/*.json'.format(working_directory)):
    with open(config) as f:
        params = json.load(f)
    # Check if the forecasting is needed for this configuration
    if not 'forecasting' in params:
        exit(0)
    try:
        locations = utils.read_locations(params['forecasting'])
    except Exception as e:
        print("Unable to load locations for config {}: {}".format(config, e))
        continue
    # Download the data and upload it to Mongo
    for loc in locations:
        # Download the meteo forecastings
        r = forecast_weather(params['keys']['darksky'], loc[1], loc[2], csv_export=True, wd=data_directory)
