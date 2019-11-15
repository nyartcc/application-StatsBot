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

            check_query = "SELECT * FROM `statistics_hours` WHERE year={0} AND month={1} AND minutes>0".format(
                i, j)

            cur = mydb.cursor()
            cur.execute(check_query)
            cur.fetchall()
            rc = cur.rowcount

            print("Rowcount: {0}".format(rc))

            if rc == 0:
                insert_query = "INSERT INTO `statistics_hours` (`year`, `month`, minutes`) VALUES ({0}, {1}, {2});".format(
                    i, j, hours)
                insert_run = cur.execute(insert_query)

                print(insert_run.rowcount, "records inserted.")

            if rc > 0:
                update_query = "UPDATE `statistics_hours` SET minutes={0} WHERE year={1} AND month={2};".format(
                    hours, i, j)
                update_run = cur.execute(update_query)

                print(update_run.rowcount, "updated.")

        else:
            print("{0}-{1}: Out of range".format(i, j))


f.close()
