# -*- coding: utf-8 -*-
import os
import pandas as pd
import tailer as tl
import io
import numpy as np


def read_locations(params, mongo_connection=None):
    # Create the location list to download
    if 'list' in params['locations']:
        locations = []
        for loc in params['locations']['list']:
            if 'stationId' in loc:
                locations.append([loc['stationId'], loc['lat'], loc['lon']])
            else:
                locations.append([None, loc['lat'], loc['lon']])
    elif 'file' in params['locations']:
        try:
            sep = params['locations']['file']['sep'] if 'sep' in params['locations']['file'] else ","
            header = params['locations']['file']['header'] if 'header' in params['locations']['file'] else None
            columns = params['locations']['file']['columns'] if 'columns' in params['locations']['file'] else None
            df = pd.read_csv(params['locations']['file']['filename'], sep=sep, header=header, names=columns)
            locations = [[row[1][params['locations']['file']['station_column']],
                          float(row[1][params['locations']['file']['lat_column']]),
                          float(row[1][params['locations']['file']['lon_column']])] for k, row in df.iterrows()]
        except Exception as e:
            raise Exception("File configuration is not correct {}".format(e))
    elif 'mongo' in params['locations']:
        stations = mongo_connection[params['locations']['mongo']['collection']].find(
            params['locations']['file']['query'],
            {params['locations']['file']['station_column']: 1,
             params['locations']['file']['lat_column']: 1,
             params['locations']['file']['lon_column']: 1
             }
        )
        locations = [[s[params['locations']['file']['station_column']],
                      float(s[params['locations']['file']['lat_column']]),
                      float(s[params['locations']['file']['lon_column']])] for s in stations]
    else:
        raise Exception("locations must be specified")
    return locations

def read_last_csv(file, n):
    f = open(file)
    last_data = tl.head(f, 1)
    body = tl.tail(f, n)
    f.close()
    if last_data[0] == body[0]:
        last_data = body
    else:
        last_data.extend(body)
    df = pd.read_csv(io.StringIO(u'\n'.join(last_data)))
    return df


def remove_last_lines_csv(file, n):
    count = 0
    with open(file, 'r+b') as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        while f.tell() > 0:
            f.seek(-1, os.SEEK_CUR)
            char = f.read(1)
            if char != '\n' and f.tell() == end:
                print "No change: file does not end with a newline"
                return
            if char == '\n':
                count += 1
            if count == n + 1:
                f.truncate()
                print "Removed " + str(n) + " lines from end of file"
                return
            f.seek(-1, os.SEEK_CUR)


def scrap_data(columns, rows, **kwargs):
    records = []
    for row in rows:
        data_dict = {}
        for key, selector, index, treatment in columns:
            try:
                val = row.select(selector)[index].text
                for t in treatment:
                    t1 = list(t)
                    t1[t1.index("arg")] = val
                    func = t1[0]
                    val = func(*t1[1:])
                data_dict[key] = val
            except:
                data_dict[key] = np.NaN
        data_dict.update(**kwargs)
        records.append(data_dict)
    return records
