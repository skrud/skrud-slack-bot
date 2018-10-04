"""
Logic for the bot itself.

This does the meat of the bot processing.
"""

import boto3
import io
import json
import logging
import os
import re

from alpha_vantage.timeseries import TimeSeries
import slackclient


BOT_USER_API_KEY = os.environ['BOT_USER_API_KEY']
ALPHA_VANTAGE_API_KEY = os.environ['ALPHA_VANTAGE_API_KEY']
GRAPH_FUNCTION_ARN = os.environ['GRAPH_FUNCTION_ARN']


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _send_slack_message(channel, message_text):
    sc = slackclient.SlackClient(BOT_USER_API_KEY)
    sc.api_call(
        'chat.postMessage',
        channel=channel,
        text=message_text
    )


def _get_data(symbol: str):
    ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY)
    return ts.get_intraday(symbol=symbol)


def _get_current_value_of_stock(symbol, data, metadata):
    last_data_point_key = max(data.keys())
    last_data_point = data[last_data_point_key]
    last_refreshed = metadata.get('3. Last Refreshed', None)

    logger.info("Stock symbol:%s last_data_point:%s last_refreshed:%s",
                symbol, last_data_point, last_refreshed)
    return last_data_point, last_data_point_key, last_refreshed


def _get_graph_data(symbol, data, metadata):
    last_refreshed = metadata.get('3. Last Refreshed', '')

    dates = sorted(data.keys())
    return {
        'xaxis': dates,
        'yaxis': [float(data[d]['4. close']) for d in dates],
        'title': "{} (Last Refreshed {})".format(symbol, last_refreshed),
        'xlabel': "Time",
        'ylabel': "$"
    }


def lambda_handler(event, context):
    slack_event = json.loads(event['body'])['event']
    sc = slackclient.SlackClient(BOT_USER_API_KEY)

    message_text = slack_event['text']
    stock_quote_match = re.search(r'\$(\w{,8})', message_text)
    if stock_quote_match:
        stock_symbol = stock_quote_match.group(1).upper()

        logger.info("Found stock symbol %s", stock_symbol)

        try:
            data, metadata = _get_data(stock_symbol)
            last_data_point, last_data_point_date, last_refreshed = \
                _get_current_value_of_stock(stock_symbol, data, metadata)

            graph = _get_graph_data(stock_symbol, data, metadata)

            current_value = last_data_point['4. close']
        except ValueError as e:
            logger.exception("Error getting stock info for %s", stock_symbol)

            _send_slack_message(slack_event['channel'],
                                "Error getting stock info for {}: {}".format(
                                    stock_symbol, str(e)
                                ))
            return
        else:
            message = 'Current Value for {}: {} (Last Refreshed: {})'.format(
                stock_symbol, current_value, last_refreshed
            )

            if not re.search(r'nograph', message_text):
                graph_payload = {
                    'symbol': stock_symbol,
                    'date': last_refreshed,
                    'graph': graph,
                    'message_text': message,
                    'destination': {
                        'slack_channel': slack_event['channel']
                    }
                }

                payload = io.BytesIO(json.dumps(graph_payload, ensure_ascii=False).encode('utf8'))
                res = boto3.client('lambda').invoke(
                    FunctionName=GRAPH_FUNCTION_ARN,
                    Payload=payload,
                    InvocationType='Event'
                )
                logger.info("Invoked lambda: %s", str(res))
            else:
                logger.info("Sending Slack message to channel %s", slack_event['channel'])
                _send_slack_message(slack_event['channel'], message)
            return

    sc.api_call(
        'chat.postMessage',
        channel=slack_event['channel'],
        text=json.dumps(slack_event)
    )
