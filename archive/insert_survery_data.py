import csv
import pymysql.cursors
from tqdm import tqdm
from os import error, walk
import pandas as pd
import geopy
from geopy.extra.rate_limiter import RateLimiter
import time
from dotenv import dotenv_values
import sys

config = dotenv_values('.env')

filenames = next(walk('./data/'), (None, None, []))[2]

facebook_data_schema = ['EIN', 'ADDRESS', 'CITY',
                        'STATE', 'ZIP', 'ORG', 'LATITUDE', 'LONGITUDE']


def insert_into_table(table_name, schema, mydb):
    counter_val = 0

    insert_head = 'INSERT INTO ' + str(table_name)
    cols = '(EIN, ADDRESS, CITY, STATE, ZIP, ORG)'

    df = pd.read_csv('data/survey.csv')
    for index, item in df.iterrows():
        if 'GA' in item[4]:
            values = 'VALUES (' + str(float(item[0])) + ",'" + str(item[2]) + \
                "','" + str(item[3]) + "','" + str(item[4]) + "','" + \
                str(item[5]) + "','" + str(item[6]) + "');"

            with mydb.cursor() as cursor:
                comm = str(insert_head) + ' ' + str(cols) + ' ' + \
                    str(values)
                print(comm)
                # cursor.execute(
                #     f'{insert_head} {cols} {values} {dup} {complete_update}')
                cursor.execute(comm)
                if (counter_val % 5000 == 0):
                    print(counter_val)
                counter_val = counter_val + 1

    mydb.commit()


if __name__ == '__main__':

    mydb = pymysql.connect(host=config['MYSQL_HOST'],
                           user=config['MYSQL_USER'],
                           password=config['MYSQL_PASSWORD'],
                           db=config['MYSQL_DB_NAME'])

    insert_into_table('survey',
                      facebook_data_schema,
                      mydb)
    mydb.close()
