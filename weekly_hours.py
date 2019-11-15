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

today = datetime.date.today()
week_ago = today - datetime.timedelta(days=7)

print("Week ago: {}{}".format(week_ago.year, week_ago.month))
print("Today: {}{}".format(today.year, today.month))


for i in range(week_ago.year, today.year + 1):
    for j in range(week_ago.month, today.month + 1):

        start_time = datetime.datetime(
            week_ago.year, week_ago.month, week_ago.day, 0, 0).timestamp()
        end_time = datetime.datetime(
            today.year, today.month, today.day, 0, 0).timestamp()

        mycursor = mydb.cursor()
        my_query = "SELECT SUM(duration) FROM connections WHERE logon_time > {} AND logon_time < {};".format(
            start_time, end_time)
        run = mycursor.execute(my_query)
        hours = convertTuple(mycursor.fetchone())

        if (hours != None):
            print("{}-{}-{}: {} -- {}-{}".format(i, j, today.day,
                                                 round(hours / 60 / 60, 1), start_time, end_time))

            check_query = "SELECT * FROM `statistics_weekly_hours` WHERE year={0} AND month={1} AND day={2} AND minutes={3}".format(
                i, j, today.day, hours)

            cur = mydb.cursor()
            cur.execute(check_query)
            records = cur.fetchall()
            rc = cur.rowcount

            if rc == 0:
                insert_query = "INSERT INTO statistics_weekly_hours (year, month, day, minutes) VALUES ({0},{1},{2},{3});".format(
                    i, j, today.day, round(hours, 6))
                insert_run = cur.execute(insert_query)

                print(cur.rowcount, "records inserted.")

            if rc > 0:

                for row in records:
                    current_hours = row[4]
                    if current_hours == hours:
                        print("No Update required.")
                    else:

                        update_query = "UPDATE `statistics_weekly_hours` SET minutes={0} WHERE year={1} AND month={2} AND day={3};".format(
                            hours, i, j, today.day)
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
                                            "text": "*Details*\nThe table 'statistics_weekly_hours' was updated with new data. Please verify that the data is correct. If not, use the data below to rectify the issue."
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
                                            "text": "*Old Data*\n Year: {0}\n Month:{1} \n Day: {4} Minutes:{2} \n\n *New Data*\n Year: {0} \n Month: {1} \n Minutes: {3}".format(i, j, current_hours, hours, today.day)
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

        # Top Controller
        tc_query = "SELECT SUM(duration)/60/60 AS hours, cid, controllers.fname, controllers.lname FROM connections INNER JOIN controllers USING (cid) WHERE logon_time > {} and logon_time < {} GROUP BY cid ORDER BY hours DESC;".format(start_time, end_time)
        tc = mydb.cursor()
        tc.execute(tc_query)
        tc_records = tc.fetchall()

        for row in tc_records:
            tc_hours = row[0]
            tc_cid = row[1]
            tc_fname = row[2]
            tc_lname = tow[3]

        # Get info about last weeks hours.
        previous_week_query = "SELECT * FROM `statistics_weekly_hours` WHERE year={} AND month={} AND day={}".format(
            i, j, week_ago.day)
        prev = mydb.cursor()
        prev.execute(previous_week_query)
        prev_records = prev.fetchall()

        for row in prev_records:
            day = row[3]
            prev_minutes = round(row[4] / 60 / 60, 1)
        current_hours = round(current_hours / 60 / 60, 1)

        webhook_url = 'https://hooks.slack.com/services/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' #os.getenv('SLACK_WEBHOOK_GENERAL')

        if current_hours > prev_minutes:
            message_data = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Weekly Controller Hours:*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Details*\n*Week of:* {0}.{1}.{2} \n Total Controller Hours: {3}. \n _This is UP from the previous week._ \n\n *Previous Week:* \n *Week of:* {4}.{5}.{6} \n Total Controller Hours: {7}".format(today.year, today.month, today.day, current_hours, week_ago.year, week_ago.month, week_ago.day, prev_minutes)
                        },
                        "accessory": {
                            "type": "image",
                            "image_url": "https://image.prntscr.com/image/mTFpZeXOR8_lGUTO8gVg-Q.png",
                            "alt_text": "Statistics Icon"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*This Weeks Top Controller*\n {0} {1} - CID: {2} with {3} hours! Congratulations!".format(tc_fname, tc_lname, tc_cid, tc_hours)
                        }
                    }
                ]
            }
        elif current_hours < prev_minutes:
            message_data = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Weekly Controller Hours:*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Details*\nWeek of {0}.{1}.{2} \n Total Controller Hours: {3}. \n This is DOWN from the previous week. \n\n Previous Week: \n Week of: {4}.{5}.{6} \n Total Controller Hours: {7}".format(today.year, today.month, today.day, current_hours, week_ago.year, week_ago.month, week_ago.day, prev_minutes)
                        },
                        "accessory": {
                            "type": "image",
                            "image_url": "https://image.prntscr.com/image/mTFpZeXOR8_lGUTO8gVg-Q.png",
                            "alt_text": "Statistics Icon"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*This Weeks Top Controller*\n {0} {1} - CID: {2} with {3} hours! Congratulations!".format(tc_fname, tc_lname, tc_cid, tc_hours)
                        }
                    }
                ]
            }
        else:
            message_data = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Weekly Controller Hours:*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Details*\nWeek of {0}.{1}.{2} \n Total Controller Hours: {3}. \n This is the same from the previous week, or something went horribly wrong. Someone should _probably_ take a look at it. No rush. \n\n Previous Week: \n Week of: {4}.{5}.{6} \n Total Controller Hours: {7}".format(today.year, today.month, today.day, current_hours, week_ago.year, week_ago.month, week_ago.day, prev_minutes)
                        },
                        "accessory": {
                            "type": "image",
                            "image_url": "https://image.prntscr.com/image/mTFpZeXOR8_lGUTO8gVg-Q.png",
                            "alt_text": "Statistics Icon"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*This Weeks Top Controller*\n {0} {1} - CID: {2} with {3} hours! Congratulations!".format(tc_fname, tc_lname, tc_cid, tc_hours)
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
