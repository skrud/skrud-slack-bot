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


BOT_USER_API_KEY = os.environ.get('BOT_USER_API_KEY', None)
ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY', None)
GRAPH_FUNCTION_ARN = os.environ.get('GRAPH_FUNCTION_ARN', None)


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StockData(object):
    INTERVALS = {
        'intraday': 'get_intraday',
        'daily': 'get_daily',
        'weekly': 'get_weekly',
        'monthly': 'get_monthly'
    }

    def __init__(self, symbol: str, interval=None, interval_length=None):
        self.symbol = symbol

        if interval and interval not in StockData.INTERVALS:
            raise ValueError("'%s' is not a valid interval.", interval)

        self.interval = interval or 'intraday'
        self.interval_length = interval_length
        self.ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY)
        self._data = self._metadata = None

    def __load(self):
        f = getattr(self.ts, StockData.INTERVALS[self.interval])
        self._data, self._metadata = f(self.symbol)

    @staticmethod
    def to_interval(term: str):
        term_map = {
            'days': 'daily',
            'months': 'monthly',
            'weeks': 'weekly'
        }

        if term in term_map:
            return term_map[term]

        logger.warn("No such term '%s' returning intraday", term)

        return 'intraday'

    @property
    def data(self):
        if not self._data:
            self.__load()
        return self._data

    @property
    def metadata(self):
        if not self._metadata:
            self.__load()
        return self._metadata

    @property
    def current_value(self):
        last_data_point_key = max(self.data.keys())
        last_data_point = self.data[last_data_point_key]

        return last_data_point['4. close']

    @property
    def last_refreshed(self):
        last_refreshed = self.metadata.get('3. Last Refreshed', None)

        return last_refreshed

    def graph(self):
        dates = sorted(self.data.keys())
        if self.interval_length:
            dates = dates[-self.interval_length:]
        return {
            'xaxis': dates,
            'yaxis': [float(self.data[d]['4. close']) for d in dates],
            'title': "{} (Last Refreshed {})".format(self.symbol,
                                                     self.last_refreshed),
            'xlabel': "Time",
            'ylabel': "$"
        }


def _send_slack_message(channel, message_text):
    sc = slackclient.SlackClient(BOT_USER_API_KEY)
    sc.api_call(
        'chat.postMessage',
        channel=channel,
        text=message_text
    )


def _get_stock_symbol(text: str):
    stock_quote_match = re.search(r'\$(\w{,8})', text)
    if stock_quote_match:
        stock_symbol = stock_quote_match.group(1).upper()

        logger.info("Found stock symbol %s", stock_symbol)

        return stock_symbol

    return None


def _find_interval(message_text: str):
    interval_match = re.search(r'(\d{1,3})(days|weeks|months)', message_text)
    if interval_match:
        try:
            interval_length = int(interval_match.group(1))
            interval_term = interval_match.group(2)
            logger.info("Found interval length:%d term:%s", interval_length,
                        interval_term)
            return interval_length, StockData.to_interval(interval_term)
        except ValueError:
            logger.exception("Invalid interval")

    return None, None


def lambda_handler(event, context):
    slack_event = json.loads(event['body'])['event']
    sc = slackclient.SlackClient(BOT_USER_API_KEY)

    message_text = slack_event['text']
    stock_symbol = _get_stock_symbol(message_text)

    if stock_symbol:
        interval_length, interval = _find_interval(message_text)
        sd = StockData(stock_symbol, interval=interval,
                       interval_length=interval_length)
        try:
            graph = sd.graph()

            current_value = sd.current_value
        except ValueError as e:
            logger.exception("Error getting stock info for %s", stock_symbol)

            _send_slack_message(slack_event['channel'],
                                "Error getting stock info for {}: {}".format(
                                    stock_symbol, str(e)
                                ))
            return
        else:
            message = 'Current Value for {}: {} (Last Refreshed: {})'.format(
                stock_symbol, current_value, sd.last_refreshed
            )

            if not re.search(r'nograph', message_text):
                graph_payload = {
                    'symbol': stock_symbol,
                    'date': sd.last_refreshed,
                    'graph': graph,
                    'message_text': message,
                    'destination': {
                        'slack_channel': slack_event['channel']
                    }
                }

                if interval_length and interval:
                    graph_payload['interval'] = '{}{}'.format(interval_length,
                                                              interval)
                else:
                    graph_payload['interval'] = 'intraday'

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


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log-level', choices=('DEBUG', 'INFO', 'WARN',
                                                      'ERROR', 'TRACE'),
                        default='INFO')
    parser.add_argument('-s', '--stock-symbol', type=str)
    parser.add_argument('-i', '--interval', type=str, default='')
    parser.add_argument('-o', '--output', type=str)

    options = parser.parse_args()

    logging.basicConfig(level=getattr(logging, options.log_level))

    if options.stock_symbol:
        interval_length, interval = _find_interval(options.interval)
        sd = StockData(options.stock_symbol, interval, interval_length)

        logging.info("%s datapoints", len(sd.data))
        logging.debug(sd.data)
        logging.debug(sd.metadata)
        logging.info("Current Value:%s Last Refreshed:%s", sd.current_value,
                     sd.last_refreshed)

        if options.output:
            with open(options.output, 'w') as output_file:
                json.dump(sd.graph(), output_file, indent=2)
