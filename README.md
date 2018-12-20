# beegroup_meteo

Project to upload meteo data to mongo

## Configure It!

To configure a new project into the program, set a <project>.json file in the folder `available_config` with the following information:

```json
{
	"forecasting": {
		"mongo_collection": "forecasting_meteo"
	}, //if not set, the forecasting data will not be executed
	"historical": {
		"mongo_collection": "historical_meteo",
		"timestamp_from": "2018-11-01T00:00:00Z"
	}, //if not set, the historical data will not be executed
	"mongodb": {
		"host": "host",
		"port": "port as integer",
		"username": "user",
		"password": "password",
		"db": "database_name"
	},
	"keys": {
		"darksky": "dark_sky_api_key",
		"CAMS": ["cams_mail_1", "cams_mail_2"]
	},
	"locations": {
		"list": ["41.50,2.00"], //list of lat,long strings
		"file": {
			"filename": "file_name",
			"sep": "\t",
			"columns": ["postalCode","lat","lon","heigth","country","unused1","unused2","unused3"],
			"lat_column": "lat",
			"lon_column": "lon"
		}, //csv with lat,long values
		"mongo": "collection" //collection with lat_long values.
	} //only one method must be set
}
```

Notice the comments in the previous json, as there are some rules to configure properly the project

## Rules! 

1 - "forecasting" and "historical" dictionaries are optional, they indicat that the projects requires historical or forecasting data. If none are set, the project will not do anything
2 - "locations" has different options to load locations. Only one must be set. If one or more are set, the behaviour can be unexpected.

# Enjoy the Meteo Data!