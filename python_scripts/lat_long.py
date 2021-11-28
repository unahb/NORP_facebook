import csv
import pymysql.cursors
from tqdm import tqdm
from os import error, walk
import pandas as pd
import geopy
import time
from geopy.extra.rate_limiter import RateLimiter
from dotenv import dotenv_values

config = dotenv_values(".env")


def update_lat_long(table_name, mydb):
    geolocator = geopy.geocoders.Bing(
        config["GEOPY_API_KEY"], user_agent='switch', timeout=10)
    # geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    with mydb.cursor() as cursor:
        cursor.execute('SELECT * FROM ' + table_name + " ;")
        for row in cursor:
            addr = row[1] + ", " + row[2] + ", " + row[3] + ", " + row[4]
            if "PO BOX" not in row[1] and row[12] is None and row[13] is None:
                location = geolocator.geocode(addr)
                # if location.latitude >= 33.608232 and location.latitude <= 33.925115 and location.longitude <= -84.219371 and location.longitude >= -84.513313:
                update = "UPDATE " + table_name + " SET LATITUDE = " + str(location.latitude) + \
                    ", LONGITUDE = " + str(location.longitude) + \
                    " WHERE EIN = " + str(row[0]) + ";"
                # print(update)
                cursor.execute(update)
                mydb.commit()


if __name__ == '__main__':

    mydb = pymysql.connect(host=config['MYSQL_HOST'],
                           user=config['MYSQL_USER'],
                           password=config['MYSQL_PASSWORD'],
                           db=config['MYSQL_DB_NAME'])

    update_lat_long('bmf', mydb)
    mydb.close()
