from dotenv import load_dotenv
import requests
import datetime
import calendar
import json
import operator
import os
import mysql.connector
from tools import convertTuple

# Load the database variables from the .env file.
load_dotenv()
db_host = os.getenv('DATABASE_HOST')
db_user = os.getenv('DATABASE_USER')
db_pass = os.getenv('DATABASE_PASSWORD')
webhook_url = os.getenv('SLACK_WEBHOOK_SENIOR_STAFF')


# Establish a connection to the MySQL Database using the .env variables.
mydb = mysql.connector.connect(
    host=db_host,
    user=db_user,
    passwd=db_pass,
    database="nyartcco_nyartcc"
)

# Write statistics to a CSV file as extra insurance.
f = open("statistics.csv", "w+")
f.write("year,month,hours\n")

# Create a variable 'now' that can spit out current dates and times.
now = datetime.datetime.now()
current_year = now.year
current_month = now.month

# Generate a range from 2016 which is the earliest good dataset in the db, to the current year + 1.
for i in range(2016, current_year + 1):
    # Get all the months.
    for j in range(1, 13):

        if i == current_year and j > current_month:
            break

        # The data in the database is stored in epoch (unix) time, so we need to convert it to a timestamp in order to make it searchable. Begin by figuring out how many days is in the month in question. Returns as {weekday, number of days}.
        days_of_month = calendar.monthrange(i, j)

        # We know that the first day of the month will always be '1'
        start_time = datetime.datetime(i, j, 1, 0, 0).timestamp()

        # Then we get the end-day by getting the days of the month result. It is stored as the second result in the variable.
        end_time = datetime.datetime(i, j, days_of_month[1], 0, 0).timestamp()

        # Establish a MySQL connection.
        mycursor = mydb.cursor()

        # Sum the connections in between the login times.
        my_query = "SELECT SUM(duration) FROM connections WHERE logon_time >= {} AND logon_time <= {};".format(
            start_time, end_time)

        # Run the query
        run = mycursor.execute(my_query)

        # We want the results readable, so convert the tuple to a string. We also only fetch the first result in the query.
        hours = convertTuple(mycursor.fetchone())

        # Only run if the result is valid.
        if (hours != None):

            # Write it to the CSV.
            f.write("{}-{},{}\n".format(i, j, hours))

            # Useful for debugging.
            print("{}-{}: {} -- {}-{}".format(i, j,
                                              round(hours / 60 / 60, 1), start_time, end_time))

            # Run a second MySQL query, to check if we have already inserted data for the month in question.
            check_query = "SELECT * FROM `statistics_hours` WHERE year={0} AND month={1}".format(
                i, j)

            cur = mydb.cursor()
            cur.execute(check_query)
            records = cur.fetchall()

            # Count the number of rows returned. If no data exist previously, it should return 0.
            rc = cur.rowcount

            # If this is a valid result, insert it into the satistics table.
            if rc == 0:
                insert_query = "INSERT INTO statistics_hours (year, month, minutes) VALUES ({0},{1},{2});".format(
                    i, j, round(hours, 6))
                insert_run = cur.execute(insert_query)

                # Useful for debugging.
                print(cur.rowcount, "records inserted.")

            # However, if there already exist a row for this data, let's do something about it.
            if rc > 0:

                # Let's check the actual result.
                for row in records:

                    # The number of hours in the table is in the fourth column.
                    current_hours = row[3]

                    # If the calculated number of hours is the same as is already in the database, we're good. No need to do anything.
                    if current_hours == hours:
                        print("No Update required.")

                    # However, if the results are NOT the same, update it.
                    else:

                        update_query = "UPDATE `statistics_hours` SET minutes={0} WHERE year={1} AND month={2};".format(
                            hours, i, j)
                        update_run = cur.execute(update_query)

                        print(cur.rowcount, "records updated.")

                        # After updating the number of hours, send an alert message to staff in Slack notifying staff that the statistics have changed. Someone should probably look at it.

                        notify_users = ['@kmoberg']
                        message_data = {
                            "blocks": [
                                {
                                    "type": "section",
                                    "text": {
                                            "type": "mrkdwn",
                                            "text": "@kmoberg \n *STATISTICS WARNING*:"
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

        # If everything succeded, you MUST commit to the DB, or changes will not have been applied.
        mydb.commit()

# Close the file. :)
f.close()
