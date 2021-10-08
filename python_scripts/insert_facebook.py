import csv
import MySQLdb
from tqdm import tqdm
from os import error, walk
import pandas as pd
import geopy
from geopy.extra.rate_limiter import RateLimiter
import time
from dotenv import dotenv_values

config = dotenv_values(".env")

filenames = next(walk("./data/"), (None, None, []))[2]

facebook_data_schema = ['LATITUDE', 'LONGITUDE', 'ZIP', 'UNDER_FIVE',
                        'SIXTY_MORE', 'MEN', 'WOMEN', 'WOMEN_15_49', 'YOUTH_15_24', 'POPULATION']


def insert_into_table(table_name, schema, mydb):
    # insert_head = 'INSERT INTO {} (' + ')'

    for file in filenames:
        insert_head = f"INSERT INTO {table_name}"
        cols = f"(LATITUDE, LONGITUDE, "
        update = f""
        if "USA_children_under_five" in file:
            cols += f"UNDER_FIVE"
            update += f"UNDER_FIVE"
        if "USA_elderly_60_plus" in file:
            cols += f"SIXTY_MORE"
            update += f"SIXTY_MORE"
        if "USA_youth_15_24" in file:
            cols += f"YOUTH_15_24"
            update += f"YOUTH_15_24"
        if "USA_men" in file:
            cols += f"MEN"
            update += f"MEN"
        if "USA_women." in file:
            cols += f"WOMEN"
            update += f"WOMEN"
        if "USA_women_of" in file:
            cols += f"WOMEN_15_49"
            update += f"WOMEN_15_49"
        if "population_usa" in file:
            cols += f"POPULATION"
            update += f"POPULATION"
        cols += f")"
        update += " = "

        print("data/"+file)
        df = pd.read_csv("data/"+file, skiprows=1)
        print("here")
        for col in df.columns:
            df[col] = df[col].astype(float)
        for index, item in df.iterrows():
            values = f"VALUES ({item[0]:.8f}, {item[1]:.8f}, {item[2]:.8f})"
            dup = f"ON DUPLICATE KEY UPDATE"
            val_update = f"{item[2]};"
            complete_update = f"{update}{val_update}"

            # print(f"{insert_head} {cols} {values} {dup} {complete_update}")

            with mydb.cursor() as cursor:
                cursor.execute(
                    f"{insert_head} {cols} {values} {dup} {complete_update}")
            mydb.commit()


if __name__ == '__main__':

    mydb = MySQLdb.connect(host=config["MYSQL_HOST"],
                           user=config["MYSQL_USER"],
                           password=config["MYSQL_PASSWORD"],
                           db=config["MYSQL_DB_NAME"])

    insert_into_table(config["MYSQL_TABLE_NAME"],
                      facebook_data_schema,
                      mydb)
    mydb.close()