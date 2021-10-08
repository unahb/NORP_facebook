import csv
import MySQLdb
from tqdm import tqdm
from os import error, walk
import pandas as pd
import geopy
import time
from geopy.extra.rate_limiter import RateLimiter
from dotenv import dotenv_values

config = dotenv_values(".env")


def update_zip(row_entry, table_name):
    if row_entry[2] is None:
        # the zip if undefined, now we update it
        insert = f"UPDATE {table_name} SET ZIP"
        location = geocode((row_entry[0], row_entry[1]))
        # print(location.address.split(" ")[-3].strip(','))
        try:
            zip = location.address.split(" ")[-3].strip(',')
        except Exception as e:
            zip = f"-1"
        print(zip)
        insert += f" = {zip} WHERE LATITUDE = {row_entry[0]} AND LONGITUDE = {row_entry[1]};"
        print(insert)
        mydb.cursor().execute(insert)
        mydb.commit()


print((config["MYSQL_HOST"]))
print(config["MYSQL_USER"])
print(config["MYSQL_PASSWORD"])
print(config["MYSQL_DB_NAME"])

geolocator = geopy.geocoders.Bing(
    config["GEOPY_API_KEY"], user_agent='switch', timeout=10)
geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)


mydb = MySQLdb.connect(host=config["MYSQL_HOST"],
                       user=config["MYSQL_USER"],
                       password=config["MYSQL_PASSWORD"],
                       db=config["MYSQL_DB_NAME"])

query = "SELECT * from facebook"

with mydb.cursor() as cursor:
    cursor.execute(query)
    row_entry = cursor.fetchone()
    while row_entry is not None and row_entry[2] is not None:
        update_zip(row_entry, config["MYSQL_TABLE_NAME"])
        row_entry = cursor.fetchone()
        print(row_entry)
    mydb.close()
