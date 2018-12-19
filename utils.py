# -*- coding: utf-8 -*-


def read_locations(params):
    # Create the location list to download
    if 'list' in params['locations']:
        locations = [[float(j) for j in i.split(",")] for i in params['locations']['list']]
    elif 'file' in params['locations']:
        raise NotImplementedError("reading from file not implemented yet")
    elif 'mongo' in params['locations']:
        raise NotImplementedError("reading from mongo not implemented yet")
    else:
        raise Exception("locations must be specified")
    return locations

