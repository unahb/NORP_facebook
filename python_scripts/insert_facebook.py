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

facebook_data_schema = ['LATITUDE', 'LONGITUDE', 'ZIP', 'UNDER_FIVE',
                        'SIXTY_MORE', 'MEN', 'WOMEN', 'WOMEN_15_49', 'YOUTH_15_24', 'POPULATION']


def insert_into_table(table_name, schema, mydb):
    # insert_head = 'INSERT INTO {} (' + ')'

    for file in filenames:
        insert_head = 'INSERT INTO ' + str(table_name)
        cols = '(LATITUDE, LONGITUDE, '
        update = ''
        if 'USA_children_under_five' in file:
            cols += 'UNDER_FIVE'
            update += 'UNDER_FIVE'
        if 'USA_elderly_60_plus' in file:
            cols += 'SIXTY_MORE'
            update += 'SIXTY_MORE'
        if 'USA_youth_15_24' in file:
            cols += 'YOUTH_15_24'
            update += 'YOUTH_15_24'
        if 'USA_men' in file:
            cols += 'MEN'
            update += 'MEN'
        if 'USA_women_2020' in file:
            cols += 'WOMEN'
            update += 'WOMEN'
        if 'USA_women_of' in file:
            cols += 'WOMEN_15_49'
            update += 'WOMEN_15_49'
        if 'population_usa' in file:
            cols += 'POPULATION'
            update += 'POPULATION'
        cols += ')'
        update += ' = '

        print('data/'+file)
        df = pd.read_csv('data/'+file, skiprows=1)
        print('here')
        for col in df.columns:
            df[col] = df[col].astype(float)
        for index, item in df.iterrows():
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
                    str(values) + ' ' + str(dup) + ' ' + str(complete_update)
                # cursor.execute(
                #     f'{insert_head} {cols} {values} {dup} {complete_update}')
                try:
                    cursor.execute(comm)
                except:
                    print(comm)
                    sys.exit()

            mydb.commit()


if __name__ == '__main__':

    mydb = pymysql.connect(host=config['MYSQL_HOST'],
                           user=config['MYSQL_USER'],
                           password=config['MYSQL_PASSWORD'],
                           db=config['MYSQL_DB_NAME'])

    insert_into_table(config['MYSQL_TABLE_NAME'],
                      facebook_data_schema,
                      mydb)
    mydb.close()
