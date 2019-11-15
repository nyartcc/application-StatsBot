import json, requests
import datetime

current_date = datetime.datetime.today()
weeknumber = current_date.strftime("%U")

webhook_url = 'https://hooks.slack.com/services/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'
message_data = {
	"blocks": [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "ZNY Statistics week #{0}".format(weeknumber)
			}
		},
		{
			"type": "section",
			"block_id": "section567",
			"text": {
				"type": "mrkdwn",
				"text": "*Member Data*\n:male-scientist: 113\tMembers \n :female-scientist: 13  \tVisitors. \n :man-woman-boy-boy: 12  \tMembers are marked as LOA."
			},
			"accessory": {
				"type": "image",
				"image_url": "https://image.prntscr.com/image/xkd5HTpdS7GEroH48mgyzA.png",
				"alt_text": "Statistics Icon"
			}
		},
		{
			"type": "section",
			"block_id": "section789",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Staff Data*\n :guardsman: 6  \tSenior Staff \n :male_mage: 3  \tInstructors \n :male-teacher: 12\tMentors"
				}
			]
		}
	]
}

response = requests.post(
    webhook_url, data=json.dumps(message_data),
    headers={'Content-type':'application/json'}
)
if response.status_code != 200:
    raise ValueError(
    'Request to Slack returned an error %s, the response is:\n%s'
    %(reponse.status_code, response.text)
)

