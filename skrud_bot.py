"""
Logic for the bot itself.

This does the meat of the bot processing.
"""

import json
import logging
import os
import re

from alpha_vantage.timeseries import TimeSeries
import slackclient


BOT_USER_API_KEY = os.environ['BOT_USER_API_KEY']
ALPHA_VANTAGE_API_KEY = os.environ['ALPHA_VANTAGE_API_KEY']


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _send_slack_message(channel, message_text):
    sc = slackclient.SlackClient(BOT_USER_API_KEY)
    sc.api_call(
        'chat.postMessage',
        channel=channel,
        text=message_text
    )


def _get_current_value_of_stock(symbol: str):
    ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY)
    data, metadata = ts.get_intraday(symbol=symbol)

    last_data_point_key = max(data.keys())
    last_data_point = data[last_data_point_key]
    last_refreshed = metadata.get('3. Last Refreshed', None)

    logger.info("Stock symbol:%s last_data_point:%s last_refreshed:%s",
                 symbol, last_data_point, last_refreshed)
    return last_data_point, last_data_point_key, last_refreshed


def lambda_handler(event, context):
    slack_event = json.loads(event['body'])['event']
    sc = slackclient.SlackClient(BOT_USER_API_KEY)

    message_text = slack_event['text']
    stock_quote_match = re.search(r'\$(\w{,8})', message_text)
    if stock_quote_match:
        stock_symbol = stock_quote_match.group(1)

        logger.info("Found stock symbol %s", stock_symbol)

        try:
            last_data_point, last_data_point_key, last_refreshed =\
                _get_current_value_of_stock(stock_symbol)
        except ValueError as e:
            logger.exception("Error getting stock info for %s", stock_symbol)

        message = 'stock_data for {}: {} (Last Refreshed: {})'.format(
            stock_symbol, last_data_point, last_refreshed
        )
        _send_slack_message(slack_event['channel'], message)
        return

    sc.api_call(
        'chat.postMessage',
        channel=slack_event['channel'],
        text=json.dumps(slack_event)
    )
