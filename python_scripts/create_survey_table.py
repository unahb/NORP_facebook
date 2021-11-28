#!/usr/bin/python
import sys
import csv
import pymysql
from dotenv import dotenv_values

config = dotenv_values('.env')


def create_table(sql_file, mydb):
    with open(sql_file, 'r') as f:
        with mydb.cursor() as cursor:
            cursor.execute(f.read())
    mydb.commit()


if __name__ == '__main__':

    mydb = pymysql.connect(host=config["MYSQL_HOST"],
                           user=config["MYSQL_USER"],
                           password=config["MYSQL_PASSWORD"],
                           db=config["MYSQL_DB_NAME"])

    create_table('sql_scripts/create_survey.sql', mydb)

    mydb.close()
