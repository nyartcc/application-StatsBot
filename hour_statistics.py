from dotenv import load_dotenv
import requests
import datetime
import calendar
import json
import operator
import functools
import os
import mysql.connector


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

            check_query = "SELECT * FROM `statistics_hours` WHERE year={0} AND month={1}".format(
                i, j)

            cur = mydb.cursor()
            cur.execute(check_query)
            records = cur.fetchall()
            rc = cur.rowcount

            print("Rowcount: {0}".format(rc))

            if rc == 0:
                insert_query = "INSERT INTO statistics_hours (year, month, minutes) VALUES ({0},{1},{2});".format(
                    i, j, round(hours, 6))
                insert_run = cur.execute(insert_query)

                print(cur.rowcount, "records inserted.")

            if rc > 0:

                for row in records:
                    current_hours = row[3]
                    if current_hours == hours:
                        print("No Update required.")
                    else:

                        update_query = "UPDATE `statistics_hours` SET minutes={0} WHERE year={1} AND month={2};".format(
                            hours, i, j)
                        update_run = cur.execute(update_query)

                        print(cur.rowcount, "records updated.")

                        webhook_url = 'https://hooks.slack.com/services/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'
                        message_data = {
                            "blocks": [
                                {
                                    "type": "section",
                                    "text": {
                                            "type": "mrkdwn",
                                            "text": "@channel \n *STATISTICS WARNING*:"
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                            "type": "mrkdwn",
                                            "text": "*Details*\nThe table 'statistics_hours' was updated with new data. Please verify that the data is correct. If not, use the data below to rectify the issue."
                                    },
                                    "accessory": {
                                        "type": "image",
                                        "image_url": "https://image.prntscr.com/image/y9FuNIUOQBacBfIJgA0Sgg.png",
                                        "alt_text": "Statistics Icon"
                                    }
                                },
                                {
                                    "type": "section",
                                    "text": {
                                            "type": "mrkdwn",
                                            "text": "*Old Data*\n Year: {0}\n Month:{1} \n Minutes:{2} \n\n *New Data*\n Year: {0} \n Month: {1} \n Minutes: {3}".format(i, j, current_hours, hours)
                                    }

                                }
                            ]
                        }

                        response = requests.post(
                            webhook_url, data=json.dumps(message_data),
                            headers={'Content-type': 'application/json'}
                        )
                        if response.status_code != 200:
                            raise ValueError(
                                'Request to Slack returned an error %s, the response is:\n%s'
                                % (reponse.status_code, response.text)
                            )

        else:
            print("{0}-{1}: Out of range".format(i, j))

        mydb.commit()


f.close()
