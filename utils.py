# -*- coding: utf-8 -*-
import copy
import os
from datetime import datetime

import pandas as pd
import pytz
import requests
import tailer as tl
import io
import numpy as np
import logging

from dateutil.relativedelta import relativedelta
from six import StringIO

log = logging.getLogger("darksky")

def darksky_solar_stations(params, mongo_connection=None):
    if 'mongo' in params['gather_solar_radiation']:
        config = params['gather_solar_radiation']['mongo']
        stations = mongo_connection[config['collection']].find(
            {config['solar_station_column']: True},
            {'stationId': 1}
        )
        return [x['stationId'] for x in stations]
    return []


def read_locations(params, mongo_connection=None):
    # Create the location list to download
    if 'list' in params['locations']:
        locations = []
        for loc in params['locations']['list']:
            if 'stationId' not in loc:
                loc.update({'stationId': None})
            if 'lat' in loc and 'lon' in loc:
                locations.append(loc)
            else:
                log.critical("'lat' and 'lon' are mandatory fields")
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
        config = params['locations']['mongo']
        stations = mongo_connection[config['collection']].find(
            config['query'],
            {config['station_column']: 1,
             config['lat_column']: 1,
             config['lon_column']: 1
             }
        )
        locations = []
        station_ids = set()
        for s in stations:
            if s[config['station_column']] in station_ids:
                continue
            station_ids.add(s[config['station_column']])
            try:
                locations.append([
                    s[config['station_column']],
                    float(s[config['lat_column']]) if config['lat_column'] in s and s[config['lat_column']] else None,
                    float(s[config['lon_column']]) if config['lon_column'] in s and s[config['lon_column']] else None
                ])
            except Exception as e:
                raise Exception("error {}, {}, {}: {}".format(s[config['station_column']], s[config['lat_column']], s[config['lon_column']], e))
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
    with open(file, 'r+b') as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell() - 1
        for _ in range(0, n):
            while pos > 0 and f.read(1) != bytes("\n", encoding="utf-8"):
                pos -= 1
                f.seek(pos, os.SEEK_SET)
        f.truncate()


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


def MG_solar_radiation(lat, lon, ts_from, ts_to, ndays_forecasted=1,add_time_forecasting=False):
    solar_data = pd.DataFrame()

    today = datetime.utcnow()
    ts_from_ = copy.deepcopy(ts_from)
    ts_to_ = copy.deepcopy(ts_to)

    while datetime.strftime(ts_from_, "%Y-%m-%d") <= datetime.strftime(ts_to_, "%Y-%m-%d") and \
            datetime.strftime(ts_from_, "%Y-%m-%d") <= datetime.strftime(datetime.utcnow(), "%Y-%m-%d"):
        ndays = ndays_forecasted if datetime.strftime(ts_from_, "%Y-%m-%d") < datetime.strftime(today, "%Y-%m-%d") else 4
        retrieve_data = one_day_MG_rad(datetime.strftime(ts_from_, "%Y-%m-%d"), lat, lon, ndays_forecasted=ndays)
        if add_time_forecasting is True:
            retrieve_data['timeForecasting'] = np.repeat(ts_from_,len(retrieve_data))
        solar_data = pd.concat([
            solar_data,
            retrieve_data
        ])
        ts_from_ = ts_from_ + relativedelta(days=1)

    solar_data = solar_data.set_index("time")
    solar_data = solar_data[solar_data.index >= ts_from]
    # solar_data = solar_data[solar_data.index <= ts_to]
    solar_data = solar_data.reset_index()

    return solar_data


def one_day_MG_rad(day, lat, lon, run=0, ndays_forecasted=1):

    # day = "2018-02-16"
    # run = 0
    # lat = 40.0
    # lon = 4.04
    # resolution = 12

    for resolution in [(4,2),(12,2),(12,1),(36,2),(36,1)]:
        try:
            day_ = datetime.strptime(day+' 00:00:00',"%Y-%m-%d %H:%M:%S")
            ## meteogalicia stores 14 days of operational forecasts
            ## After 14 days the forecasts are moved to the WRF_HIST folder
            # test # http://mandeo.meteogalicia.es/thredds/ncss/grid//wrf_2d_04km/fmrc/files/20180423/wrf_arw_det_history_d02_20180423_0000.nc4?var=swflx&point=true&accept=csv&longitude=0.62&latitude=41.62&temporal=all
            if (datetime.utcnow() - day_).days <= 14:
                url_mg = 'http://mandeo.meteogalicia.es/thredds/ncss/grid/wrf_2d_%02ikm/fmrc/files/%s/wrf_arw_det_'\
                         'history_d0%s_%s_%02i00.nc4?var=swflx&point=true&accept=csv&longitude=%s&latitude=%s'\
                         '&temporal=all' % (
                           resolution[0],
                           datetime.strftime(day_,"%Y%m%d"),
                           resolution[1],
                           datetime.strftime(day_, "%Y%m%d"),
                           run, lon, lat
                        )
            else:
                #test# http://mandeo.meteogalicia.es/thredds/ncss/grid/modelos/WRF_HIST/d02/2018/01/wrf_arw_det_history_d02_20180122_0000.nc4?var=swflx&point=true&accept=csv&longitude=41.62&latitude=0.62&temporal=all
                ## Historical forecasts. Only run 00 is available
                url_mg = 'http://mandeo.meteogalicia.es/thredds/ncss/grid/modelos/WRF_HIST/d0%s/%s/%s/wrf_arw_det_history_d0%s_'\
                         '%s_0000.nc4?var=swflx&point=true&accept=csv&longitude=%s&latitude=%s&temporal=all' % (
                                   resolution[1],
                                   datetime.strftime(day_, "%Y"),
                                   datetime.strftime(day_, "%m"),
                                   resolution[1],
                                   datetime.strftime(day_, "%Y%m%d"),
                                   lon, lat
                               )
            r = requests.get(url_mg)
            solar_data = pd.read_csv(StringIO(r.text), sep=",")
            if len(solar_data)==0:
                raise Exception("Location out of the bounding box, trying with another resolution..."
                                "(Actual: "+str(resolution)+"km)")
            else:
                for colname in [solar_data.columns[1],solar_data.columns[2]]:
                    del solar_data[colname]
                solar_data = solar_data.rename(columns={'date':'time','swflx[unit="W m-2"]':'GHI'})
                solar_data['time'] = [pytz.UTC.localize(datetime.strptime(i, "%Y-%m-%dT%H:%M:%SZ"))\
                                           for i in solar_data.time]
                solar_data = pd.concat(
                    [pd.DataFrame(
                        [{
                            "time": pytz.UTC.localize(datetime.strptime("%sT00:00:00Z" % day,"%Y-%m-%dT%H:%M:%SZ")),
                            "GHI": 0.0
                        }]),
                    solar_data]
                )
                solar_data = solar_data.reset_index(drop=True)
                return solar_data[:(ndays_forecasted*24)]
        except Exception as e:
            log.debug(e)


def openAndSkipLines(f, symbol):
    # open a file, e.g. a CSV file, and skip lines beginning with symbol. Return the total number of lines and number of lines to skip (i.e. not containing data). If <0, file is empty
    # The file is ready to be read at the first line of data
    nbTotalLines = len(f.readlines())
    if (nbTotalLines == 0): return -1, -1
    f.seek(0,0)
    stop = False
    nbLine = 0
    while (not stop) :
        nbLine = nbLine + 1
        l = f.readline()
        if (l[0] != symbol): stop = True
    f.seek(f.tell()-len(l),0)
    nbLinesToSkip = nbLine-1
    return nbTotalLines, nbLinesToSkip



def CAMS_solar_radiation(cams_registered_mails, lat, lon, ts_from, ts_to):
    if cams_registered_mails is not None:
        day_from = datetime.strftime(ts_from, "%Y-%m-%d %H:%M:%S")[:10]
        day_to = datetime.strftime(ts_to, "%Y-%m-%d %H:%M:%S")[:10]
        for mail in cams_registered_mails:
            try:
                log.debug('Connecting with CAMS service (mail: %s)...' % mail)
                try:
                    url_cams = "http://www.soda-is.com/service/wps?Service=WPS&Request=Execute&Identifier="\
                               "get_cams_radiation&version=1.0.0&DataInputs=latitude=%s;longitude=%s;altitude=-999"\
                               ";date_begin=%s;date_end=%s;time_ref=UT;summarization=PT01H;username=%s&RawDataOutput"\
                               "=irradiation" % (
                        lat,lon,day_from,day_to,mail.replace("@","%2540")
                    )
                    r = requests.get(url_cams)
                    if r.status_code!=200:
                        raise(Exception)
                except: #In case of failure, try to connect to the pro.soda-id server, which is a mirror backup server.
                    url_cams = "http://pro.soda-is.com/service/wps?Service=WPS&Request=Execute&Identifier="\
                               "get_cams_radiation&version=1.0.0&DataInputs=latitude=%s;longitude=%s;altitude=-999"\
                               ";date_begin=%s;date_end=%s;time_ref=UT;summarization=PT01H;username=%s&RawDataOutput"\
                               "=irradiation" % (
                        lat,lon,day_from,day_to,mail.replace("@","%2540")
                    )
                    r = requests.get(url_cams)
                solar_data = pd.read_csv(StringIO(r.text),skiprows=openAndSkipLines(StringIO(r.text), "#")[1]-1,sep=";")
                solar_data["timestamp"] = [elem[0:19] for elem in solar_data["# Observation period"]]
                del solar_data["# Observation period"]
                solar_data['timestamp'] = [pytz.UTC.localize(datetime.strptime(i, "%Y-%m-%dT%H:%M:%S"))\
                                       for i in solar_data.timestamp]
                solar_data = solar_data[solar_data.timestamp >= ts_from]
                solar_data = solar_data[solar_data.timestamp <= ts_to]
                solar_data['timestamp'] = [i.astimezone(pytz.UTC)\
                                       for i in solar_data.timestamp]
                solar_data = solar_data.rename(columns={'timestamp':'time'})
                log.debug('Solar radiation data correctly downloaded!')
                return solar_data
            except Exception as e:
                log.debug(e)


def get_solar_radiation(df_hourly, config, lat, lon):
    ts_from_solar = min(df_hourly.time)
    ts_to_solar = max(df_hourly.time)
    log.debug('Obtaining the solar radiation data...')
    solar_data = CAMS_solar_radiation(
        cams_registered_mails=config['cams']['registered_emails'],
        lat=lat,
        lon=lon,
        ts_from=ts_from_solar,
        ts_to=ts_to_solar,
    )
    # If has not been possible to download the CAMS solar radiation data, try to download the historical forecasting
    # data from Meteogalicia
    if solar_data is None:
        solar_data = MG_solar_radiation(
            lat=lat,
            lon=lon,
            ts_from=ts_from_solar,
            ts_to=ts_to_solar,
            ndays_forecasted=1,
            add_time_forecasting=False
        )
    return solar_data
