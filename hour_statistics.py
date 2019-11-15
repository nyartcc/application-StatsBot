import mysql.connector
import os
import functools
import operator
from dotenv import load_dotenv
import json
import requests
import datetime
import calendar


def convertTuple(tup):
    str = functools.reduce(operator.add, (tup))
    return str


load_dotenv()
db_host = os.getenv('DATABASE_HOST')
db_user = os.getenv('DATABASE_USER')
db_pass = os.getenv('DATABASE_PASSWORD')


mydb = mysql.connector.connect(
    host=db_host,
    user=db_user,
    passwd=db_pass,
    database="nyartcco_nyartcc"
)

f = open("statistics.csv", "w+")
f.write("year,month,hours\n")
for i in range(2016, 2020):
    for j in range(1, 13):
        days_of_month = calendar.monthrange(i, j)
        start_time = datetime.datetime(i, j, 1, 0, 0).timestamp()
        end_time = datetime.datetime(i, j, days_of_month[1], 0, 0).timestamp()

        mycursor = mydb.cursor()
        my_query = "SELECT SUM(duration) FROM connections WHERE logon_time > {} AND logon_time < {};".format(
            start_time, end_time)
        run = mycursor.execute(my_query)
        hours = convertTuple(mycursor.fetchone())

        if (hours != None):
            f.write("{}-{},{}\n".format(i, j, hours))
            print("{}-{}: {} -- {}-{}".format(i, j,
                                              round(hours / 60 / 60, 1), start_time, end_time))
        else:
            print("Out of range")


f.close()
