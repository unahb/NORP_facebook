import csv
import pymysql.cursors
from tqdm import tqdm
import os
from os import error, walk
import pandas as pd
import geopy
from geopy.extra.rate_limiter import RateLimiter
import time
from dotenv import dotenv_values
import sys

config = dotenv_values('.env')

filenames = next(walk('./data/women/'), (None, None, []))[2]

facebook_data_schema = ['LATITUDE', 'LONGITUDE', 'ZIP', 'UNDER_FIVE',
                        'SIXTY_MORE', 'MEN', 'WOMEN', 'WOMEN_15_49', 'YOUTH_15_24', 'POPULATION']


def insert_into_table(table_name, schema, mydb):
    # insert_head = 'INSERT INTO {} (' + ')'
    # counter_val = 0

    for file in filenames:
        insert_head = 'INSERT INTO ' + str(table_name)
        cols = '(LATITUDE, LONGITUDE, WOMEN_15_49)'
        update = 'WOMEN_15_49 = '

        # print('data/women/'+file)
        df = pd.read_csv('data/women/'+file)
        # print('here')
        counter_val = 0
        for col in df.columns:
            df[col] = df[col].astype(float)
        for index, item in df.iterrows():
            if item[0] >= 33.608232 and item[0] <= 33.925115 and item[1] <= -84.219371 and item[1] >= -84.513313:
                values = 'VALUES (' + str(float("{:.8f}".format(item[0]))) + ',' + str(float(
                    "{:.8f}".format(item[1]))) + ',' + str(float("{:.8f}".format(item[2]))) + ')'

                # values = f'VALUES ({item[0]:.8f}, {item[1]:.8f}, {item[2]:.8f})'
                dup = 'ON DUPLICATE KEY UPDATE'
                val_update = str(item[2]) + ';'
                complete_update = str(update) + ' ' + str(val_update)

                # print(f'{insert_head} {cols} {values} {dup} {complete_update}')
                # print(complete_update)

                with mydb.cursor() as cursor:
                    comm = str(insert_head) + ' ' + str(cols) + ' ' + \
                        str(values) + ' ' + str(dup) + \
                        ' ' + str(complete_update)
                    # cursor.execute(
                    #     f'{insert_head} {cols} {values} {dup} {complete_update}')
                    try:
                        cursor.execute(comm)
                        # print(comm)
                        # if (counter_val % 5000 == 0):
                        # print(counter_val)
                        # mydb.commit()
                        # counter_val = counter_val + 1
                    except Exception as e:
                        mydb.rollback()

        mydb.commit()
        # print(counter_val)
        os.remove('data/women/'+file)


if __name__ == '__main__':

    mydb = pymysql.connect(host=config['MYSQL_HOST'],
                           user=config['MYSQL_USER'],
                           password=config['MYSQL_PASSWORD'],
                           db=config['MYSQL_DB_NAME'])

    insert_into_table(config['MYSQL_TABLE_NAME'],
                      facebook_data_schema,
                      mydb)
    mydb.close()
