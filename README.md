# beegroup_meteo

Project to upload meteo data to mongo

## Configure It!

To configure a new project into the program, set a <project>.json file in the folder `available_config` with the following information:

```json
{
	"forecasting": {
		"mongo_collection": "forecasting_meteo",
		"locations": {
		  "list": [{"stationId": "40,2", "lat": 40, "lon": 2}], //list of lat,long strings
            "file": {
                "filename": "file_name",
                "sep": "\t",
                "columns": ["postalCode","lat","lon","heigth","country","unused1","unused2","unused3"],
                "lat_column": "lat",
                "lon_column": "lon",
                "station_column": "postalCode"
            }, //csv with lat,long values
		  "mongo": {
		        "collection" : collection, //collection with lat_long values.
		        "lat_column": "lat",
                "lon_column": "lon",
                "station_column": "postalCode",
           }
	    } //only one method must be set
	}, //if not set, the forecasting data will not be executed
	"historical": {
		"mongo_collection": "historical_meteo",
		"timestamp_from": "2018-11-01T00:00:00Z",
		"locations": {
            "list": [{"stationId": "40,2", "lat": 40, "lon": 2}], //list of lat,long strings
            "file": {
                "filename": "file_name",
                "sep": "\t",
                "columns": ["postalCode","lat","lon","heigth","country","unused1","unused2","unused3"],
                "lat_column": "lat",
                "lon_column": "lon",
                "station_column": "postalCode"
            }, //csv with lat,long values
            "mongo": {
		        "collection" : collection, //collection with lat_long values.
		        "lat_column": "lat",
                "lon_column": "lon",
                "station_column": "postalCode",
           }
        } //only one method must be set
	}, //if not set, the historical data will not be executed
	"mongodb": {
		"host": "host",
		"port": "port as integer",
		"username": "user",
		"password": "password",
		"db": "database_name",
		"stations_collection": "weather_stations" //collection to store the last time of each query

	},
	"keys": {
		"darksky": "dark_sky_api_key",
		"CAMS": ["cams_mail_1", "cams_mail_2"]
	}
}
```

Notice the comments in the previous json, as there are some rules to configure properly the project

## Rules! 

1 - "forecasting" and "historical" dictionaries are optional, they indicat that the projects requires historical or forecasting data. If none are set, the project will not do anything
2 - "locations" has different options to load locations. Only one must be set. If one or more are set, the behaviour can be unexpected.

# Enjoy the Meteo Data!


# Server configuration.

Set the following cronjobs in the server to have the project running properly:

