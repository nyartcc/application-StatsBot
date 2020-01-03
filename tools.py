import functools
import operator
from discord_webhook import DiscordWebhook, DiscordEmbed
import os
from dotenv import load_dotenv

discord_webhook = os.getenv('DISCORD_WEBHOOK_GENERAL')


def convertTuple(tup):
    str = functools.reduce(operator.add, (tup))
    return str


def post_to_discord(last_week_hours, this_week_hours, tc_fname, tc_lname, tc_cid, tc_hours):
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

    # Post fun stuff to Discord
    webhook = DiscordWebhook(url=discord_webhook)

    # add embed object to webhook
    webhook.add_embed(embed)
    webhook.add_embed(embed2)
    webhook.execute()
