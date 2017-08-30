# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 17:37:51 2017

@author: qwang2
"""

import sys
import time
from datetime import date
from dateutil.relativedelta import relativedelta
import configparser
import logging
import traceback
import argparse

from io import StringIO
import pandas as pd
from psycopg2 import connect
import psycopg2.sql as pgsql
from psycopg2.extras import execute_values
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger('upload_mto_data')

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.setLevel(logging.INFO)


def parse_args(args):
    """Parse command line arguments
    
    Args:
        sys.argv[1]: command line arguments
        
    Returns:
        dictionary of parsed arguments
    """
    parser = argparse.ArgumentParser(description='Fetch month of MTO volume data')
    
    parser.add_argument('--yyyymm',
                        help='Specify month to fetch data as YYYYMM,'\
                        ' program defaults to last month')
    
    return parser.parse_args(args)


class MTOVolumeScraper( object ):

    def __init__(self, dbset, auth):
        '''Create a MTOVolumeScraper to iterate over months and pull volume data

        Parameters:
            dbset: dictionary to create a db connection
            auth: dictionary to authenticate when getting data from url

        '''
        super(MTOVolumeScraper, self).__init__()
        self.db = connect(**dbset)
        self.auth = HTTPBasicAuth(auth['username'], auth['password'])
        self.baseurl = auth['baseurl']
        self.sensors = self.get_sensors()


    def get_sensors(self):
        '''Query database for list of sensors'''
        logger.debug('Getting sensors from database')
        get_sql = "SELECT detector_id as detectorid FROM mto.sensors"
        with self.db.cursor() as cur:
            cur.execute(get_sql)
            return cur.fetchall()


    def get_and_process_data(self, year, month):
        '''
        Send a request for each MTO sensor for the given year, month.
        :param year:
            year to grab data for
        :param month:
            month to grab data for
        '''
        table = []

        # Get data for each sensor
        for s in self.sensors:
            s = s[0]
            params = {'year': year,
                      'month': month,
                      'reportType': 'min_30',
                      'sensorName': s}
            try:
                data = requests.get(url=self.baseurl,
                                    params=params,
                                    auth=self.auth,
                                    timeout=10).text
            except requests.exceptions.RequestException as err:
                logger.critical(err)
                continue
            # if there's no data for the sensor
            if len(data) == 0:
                logger.error(s + 'not found in' + str(year) + str(month))
                continue
            # Find number of headerlines
            #(there are two versions depending on whether lat/lon are in)
            count = 0
            for line in data.split('\n'):
                if line[:4] == 'Time':
                    break
                count = count + 1
            if count == len(data.split('\n')):
                logger.error(s + ' not found in ' + str(year) + '-' + str(month))
                continue
            else:
                data = pd.read_csv(StringIO(data),
                                   skiprows=count,
                                   nrows=48,
                                   index_col=0)
            # Format the data and append to lists
            data = data.transpose()
            for dt in data.index:
                if dt != ' ':
                    for v,t in zip(data.loc[dt], data.columns):
                        table.append([s, dt + ' ' + t, v])

        # Create table -> Truncate (if exists) -> Create partition rules
        
        if month < 10:
            month = '0' + str(month)
        else:
            month = str(month)
        
        tablename = pgsql.Identifier('mto.mto_agg_30_' + str(year) + month)
        rulename = pgsql.Identifier('mto_insert_' + str(year) + month)
        start_date = pgsql.Literal(str(year)+'-'+month+'01')

        logger.info('Data pulled successfully, sending to database...')
        
        sql_create = pgsql.SQL('''CREATE TABLE IF NOT EXISTS {tablename} 
        (PRIMARY KEY (detector_id, count_bin),
        CHECK count_bin >= {start_date}::TIMESTAMP AND
             count_bin < {start_date}::TIMESTAMP + '1 mon'::interval)
        INHERITS (mto.mto_agg_30);''')
        sql_crrule = pgsql.SQL('''CREATE OR REPLACE RULE {rulename}
        AS ON INSERT TO mto.mto_agg_30
        WHERE new.count_bin >= {start_date}::TIMESTAMP
        AND new.count_bin < {start_date}::TIMESTAMP + '1 mon'::interval
        DO INSTEAD INSERT INTO {tablename} (detector_id, count_bin, volume)
        VALUES (new.detector_id, new.count_bin, new.volume);''')
        sql_trunc = pgsql.SQL('TRUNCATE {tablename};')
        sql_insert = pgsql.SQL('INSERT INTO {tablename} VALUES %s')

        with self.db as con:
            with con.cursor() as cur:
                cur.execute(sql_create.format(tablename=tablename,
                                              start_date=start_date))
                cur.execute(sql_trunc.format(tablename=tablename))
                cur.execute(sql_crrule.format(tablename=tablename,
                                              rulename=rulename,
                                              start_date=start_date))
                execute_values(cur,
                               sql_insert.format(tablename=tablename),
                               table)

        logger.info(str(year) + month + ' uploaded.')


def main(yyyymm = None, **kwargs):
    '''
    
    :param yyyymm:
        Year-month to be processed
    '''
    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']
    auth = CONFIG['AUTH']
    logger.info('Connecting to Database')

    mto_scraper = MTOVolumeScraper(dbset, auth)

    logger.info('Database connected.')

    if yyyymm is None:
        #Use today's day to determine month to process
        last_month = date.today() + relativedelta(months=-1)
        year = last_month.year
        month = last_month.month
    else:
        #Process and test whether the provided yyyymm is accurate
        regex_yyyymm = re.compile(r'20\d\d(0[1-9]|1[0-2])')
        if fullmatch(regex_yyyymm.pattern, yyyymm):
            year = int(yyyymm[:4])
            month = int(yyyymm[-2:])
        else:
            raise ValueError('{yyyymm} is not a valid year-month value of format YYYYMM'
                             .format(yyyymm=yyyymm))

    mto_scraper.get_and_process_data(year, month)

    mto_scraper.db.close()

if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    try:
        main(**vars(parse_args(sys.argv[1:])))
    except Exception as exc:
        logger.critical(traceback.format_exc())