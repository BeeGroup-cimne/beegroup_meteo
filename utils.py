# -*- coding: utf-8 -*-
import pandas as pd

def read_locations(params):
    # Create the location list to download
    if 'list' in params['locations']:
        locations = [[float(j) for j in i.split(",")] for i in params['locations']['list']]
    elif 'file' in params['locations']:
        try:
            sep = params['locations']['file']['sep'] if 'sep' in params['locations']['file'] else ","
            header = params['locations']['file']['header'] if 'header' in params['locations']['file'] else None
            columns = params['locations']['file']['columns'] if 'columns' in params['locations']['file'] else None
            df = pd.read_csv(params['locations']['file']['filename'], sep=sep, header=header, names=columns)
            locations = [[float(row[1][params['locations']['file']['lat_column']]), float(row[1][params['locations']['file']['lon_column']])] for k, row in df.iterrows()]
        except Exception as e:
            raise Exception("File configuration is not correct {}".format(e))
    elif 'mongo' in params['locations']:
        raise NotImplementedError("reading from mongo not implemented yet")
    else:
        raise Exception("locations must be specified")
    return locations

