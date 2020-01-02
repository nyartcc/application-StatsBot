from dotenv import load_dotenv
import requests
import datetime
import calendar
import json
import operator
import functools
import os
import mysql.connector
from tools import convertTuple
from discord_webhook import DiscordWebhook, DiscordEmbed

# Load the database variables from the .env file.
load_dotenv()
db_host = os.getenv('DATABASE_HOST')
db_user = os.getenv('DATABASE_USER')
db_pass = os.getenv('DATABASE_PASSWORD')
webhook_url = os.getenv('SLACK_WEBHOOK_GENERAL')
discord_webhook = os.getenv('DISCORD_WEBHOOK_GENERAL')

# Establish a connection to the MySQL Database using the .env variables.
mydb = mysql.connector.connect(
    host=db_host,
    user=db_user,
    passwd=db_pass,
<<<<<<< HEAD
    database="nyartcco_nyartcc"
=======
    database="nyartcco_nyartcc"  # Change this to the appropriate database.
>>>>>>> 77e2ff35490b2d47e3a198238a77f05bf238cbc4
)

# Get some date information that will be needed later.
today = datetime.date.today()
week_ago = today - datetime.timedelta(days=7)

# Used for debugging to get the different dates.
print("Week ago: {}{}".format(week_ago.year, week_ago.month))
print("Today: {}{}".format(today.year, today.month))
current_hours = 0
prev_minutes = 0

# Generate a 7-day date range, beginning with the year.
for i in range(week_ago.year, today.year + 1):

    # Then the month
    for j in range(week_ago.month, today.month + 1):

        # Finally convert the time to epoch (unix) time as this is what is stored in the db.
        start_time = datetime.datetime(
            week_ago.year, week_ago.month, week_ago.day, 0, 0).timestamp()
        end_time = datetime.datetime(
            today.year, today.month, today.day, 0, 0).timestamp()

        # Start a MySQL Connection
        mycursor = mydb.cursor()

        # Get the connections for the last week.
        my_query = "SELECT SUM(duration) FROM connections WHERE logon_time > {} AND logon_time < {};".format(
            start_time, end_time)

        # Run it
        run = mycursor.execute(my_query)

        # Convert the result to a useful string, and fetch only the first row in the result.
        hours = convertTuple(mycursor.fetchone())

        # Check if the result is valid (not none/null)
        if (hours != None):

            # Used for debugging.
            print("{}-{}-{}: {} -- {}-{}".format(i, j, today.day,
                                                 round(hours / 60 / 60, 1), start_time, end_time))

            # Check if there already exist an entry in the statistics table for this day that also has the exact same minutes.
            check_query = "SELECT * FROM `statistics_weekly_hours` WHERE year={0} AND month={1} AND day={2} AND minutes={3}".format(
                i, j, today.day, hours)

            # Establish a new DB query connection
            cur = mydb.cursor()
            cur.execute(check_query)
            records = cur.fetchall()

            # Count the number of rows that are returned. If nothing exist in the db, the result will be 0.
            rc = cur.rowcount

            # If so, insert the statistics in the database.
            if rc == 0:
                insert_query = "INSERT INTO statistics_weekly_hours (year, month, day, minutes) VALUES ({0},{1},{2},{3});".format(
                    i, j, today.day, round(hours, 6))
                insert_run = cur.execute(insert_query)

                # Useful for debugging.
                print(cur.rowcount, "records inserted.")

            # Else, if something already exist...
            if rc > 0:

                # If the result in the DB is the exact same, no worries. No update is required.
                for row in records:
                    current_hours = row[4]
                    if current_hours == hours:
                        print("No Update required.")

                    # However, if something exist, and the hours don't match... Well, notify someone, because something is probably wrong.
                    else:

                        # Update the DB.
                        update_query = "UPDATE `statistics_weekly_hours` SET minutes={0} WHERE year={1} AND month={2} AND day={3};".format(
                            hours, i, j, today.day)
                        update_run = cur.execute(update_query)

                        # Useful for debugging.
                        print(cur.rowcount, "records updated.")

                        # Send a message to Slack.
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
        # Catch-all error.
        else:
            print("{0}-{1}: Out of range".format(i, j))

        # Commit the changes to the DB.
        mydb.commit()

        # Calculate the top controller last week. Inner join the with the controller info so we can get the name of the user.
        tc_query = "SELECT SUM(duration) AS hours, cid, controllers.fname, controllers.lname FROM connections INNER JOIN controllers USING (cid) WHERE logon_time > {} and logon_time < {} GROUP BY cid ORDER BY hours ASC;".format(
            start_time, end_time)
        tc = mydb.cursor()
        tc.execute(tc_query)
        tc_records = tc.fetchall()

        # Get the results of the query.
        for row in tc_records:

            # The number of hours the controller had last week, rounded to 1 decimal.
            tc_hours = round(row[0] / 60 / 60, 1)

            # The controller CID.
            tc_cid = row[1]

            # Controller first name
            tc_fname = row[2]

            # Controller last name
            tc_lname = row[3]

        # Get info about last weeks hours.
        previous_week_query = "SELECT * FROM `statistics_weekly_hours` WHERE year={} AND month={} AND day={}".format(
            i, j, week_ago.day)
        prev = mydb.cursor()
        prev.execute(previous_week_query)
        prev_records = prev.fetchall()

        for row in prev_records:
            day = row[3]
            previous_minutes = row[4]
            current_hours = round(current_hours / 60 / 60, 1)
            prev_minutes = round(previous_minutes / 60 / 60, 1)

            webhook_url = os.getenv('SLACK_WEBHOOK_GENERAL')
            # This is a super dirty hack. I'm sorry. Check for the number of hours, and send a different message depending on what the results are.
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

<<<<<<< HEAD
            # Post fun stuff to Discord
webhook = DiscordWebhook(url=discord_webhook)

# create embed object for webhook
embed = DiscordEmbed(title='Weekly Statistics',
                     description='Statustics for the week of {}.{}.{}'.format(today.year, today.month, today.day), color=242424)

# set image
embed.set_thumbnail(
    url='https://image.prntscr.com/image/mTFpZeXOR8_lGUTO8gVg-Q.png')

current_hours = round(current_hours / 60 / 60, 1)

embed.add_embed_field(name='This Weeks Hours',
                      value='{}'.format(current_hours))
embed.add_embed_field(name='Last Weeks Hours',
                      value='{}'.format(prev_minutes))
embed2 = DiscordEmbed(title='This weeks top controller:',
                      description='{0} {1} - CID: {2} with {3} hours! Congratulations!'.format(tc_fname, tc_lname, tc_cid, tc_hours), color=242424)

# add embed object to webhook
webhook.add_embed(embed)
webhook.add_embed(embed2)
webhook.execute()
=======
        # Post fun stuff to Discord
        webhook = DiscordWebhook(url=discord_webhook)

        # create embed object for webhook
        embed = DiscordEmbed(title='Weekly Statistics',
                             description='Statustics for the week of {}.{}.{}'.format(today.year, today.month, today.day), color=242424)

        # set image
        embed.set_thumbnail(
            url='https://image.prntscr.com/image/mTFpZeXOR8_lGUTO8gVg-Q.png')

        current_hours = round(current_hours / 60 / 60, 1)

        embed.add_embed_field(name='This Weeks Hours',
                              value='{}'.format(current_hours))
        embed.add_embed_field(name='Last Weeks Hours',
                              value='{}'.format(prev_minutes))
        embed2 = DiscordEmbed(title='This weeks top controller:',
                              description='{0} {1} - CID: {2} with {3} hours! Congratulations!'.format(tc_fname, tc_lname, tc_cid, tc_hours), color=242424)

        # add embed object to webhook
        webhook.add_embed(embed)
        webhook.add_embed(embed2)
        webhook.execute()
>>>>>>> 77e2ff35490b2d47e3a198238a77f05bf238cbc4
