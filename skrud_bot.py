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
import abc

from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies
from alpha_vantage.foreignexchange import ForeignExchange
from slack_sdk import WebClient


BOT_USER_API_KEY = os.environ.get('BOT_USER_API_KEY', None)
ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY', None)
GRAPH_FUNCTION_ARN = os.environ.get('GRAPH_FUNCTION_ARN', None)


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Interval(metaclass=abc.ABCMeta):
    INTERVALS = {
        'intraday',
        'daily',
        'weekly',
        'monthly'
    }

    def __init__(self, interval=None, interval_length=None, key_name='4. close'):
        if interval and interval not in StockData.INTERVALS:
            raise ValueError("'%s' is not a valid interval.", interval)

        self.interval = interval or 'intraday'
        self.interval_length = interval_length
        self.key_name = key_name
        self._data = self._metadata = self._dates = None

    @staticmethod
    def is_valid_interval(interval):
        return interval and interval in INTERVALS

    @property
    def data(self):
        if not self._data:
            self._load()
        return self._data

    @property
    def metadata(self):
        if not self._metadata:
            self._load()
        return self._metadata

    @property
    def current_value(self):
        last_data_point_key = max(self.data.keys())
        last_data_point = self.data[last_data_point_key]

        return '{0:.2f}'.format(float(last_data_point[self.key_name]))

    @property
    def mean_value(self):
        close_values = [float(self.data[d][self.key_name]) for d in self.dates]
        if close_values:
            mean = sum(close_values) / len(close_values)
            return '{0:.2f}'.format(mean)
        return '0.0'

    @abc.abstractproperty
    def last_refreshed(self):
        pass

    @property
    def dates(self):
        if not self._dates:
            dates = sorted(self.data.keys())
            if self.interval_length:
                dates = dates[-self.interval_length:]
            self._dates = dates
        return self._dates

    @property
    def date_range(self):
        return self.dates[0], self.dates[-1]

    @abc.abstractmethod
    def _load(self):
        pass


class StockData(Interval):
    def __init__(self, symbol: str, interval=None, interval_length=None):
        self.symbol = symbol
        self.ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY)
        self._data = self._metadata = self._dates = None
        super(StockData, self).__init__(interval, interval_length, '4. close')

    def _load(self):
        f = getattr(self.ts, 'get_{}'.format(self.interval))
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
    def last_refreshed(self):
        last_refreshed = self.metadata.get('3. Last Refreshed', None)
        return last_refreshed

    def graph(self):
        start, end = self.date_range
        return {
            'xaxis': self.dates,
            'yaxis': [float(self.data[d][self.key_name]) for d in self.dates],
            'title': "{} ({} - {})".format(self.symbol,
                                           start, end),
            'xlabel': "Time",
            'ylabel': "$"
        }


class BtcData(Interval):
    def __init__(self, interval=None, interval_length=None, symbol='BTC', market='USD'):
        self.cc = CryptoCurrencies(
            key=ALPHA_VANTAGE_API_KEY)
        self.fe = ForeignExchange(key=ALPHA_VANTAGE_API_KEY)
        self.symbol = symbol
        self.market = market
        super(BtcData, self).__init__(interval or 'daily', interval_length or 7,
                                      key_name='4a. close ({})'.format(self.market))

    def _load(self):
        f = getattr(self.cc, 'get_digital_currency_{}'.format(self.interval))
        self._data, self._metadata = f(
            symbol=self.symbol, market=self.market)

        try:
            current, _ = self.fe.get_currency_exchange_rate(
                from_currency=self.symbol, to_currency=self.market)
            date_str, _ = current['6. Last Refreshed'].split(' ')
            self._data[date_str][self.key_name] = current['5. Exchange Rate']
            self._metadata['6. Last Refreshed'] = current['6. Last Refreshed']
        except ValueError as e:
            logger.info(
                'Unable to get current exchange - rate limited', exc_info=e)

    @property
    def last_refreshed(self):
        last_refreshed = self.metadata.get('6. Last Refreshed', None)
        return last_refreshed

    def graph(self):
        start, end = self.date_range
        return {
            'xaxis': self.dates,
            'yaxis': [float(self.data[d][self.key_name]) for d in self.dates],
            'title': "{} ({} - {})".format(self.symbol, start, end),
            'xlabel': "Time",
            'ylabel': "{}".format(self.market)
        }


def _send_slack_message(channel, message_text):
    sc = WebClient(token=BOT_USER_API_KEY)
    sc.chat_postMessage(
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


def _is_bitcoin(text: str):
    btc_match = re.search(r'(bitcoin|\u20bf|btc)', text)
    return btc_match


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
    message_text = slack_event['text']

    sd = None
    stock_symbol = _get_stock_symbol(message_text)
    interval_length, interval = _find_interval(message_text)
    if stock_symbol:
        sd = StockData(stock_symbol, interval=interval,
                       interval_length=interval_length)
    elif _is_bitcoin(message_text):
        stock_symbol = 'BTC'
        sd = BtcData(interval=interval, interval_length=interval_length)
    else:
        _send_slack_message(
            slack_event['channel'], "Could not find stock symbol or bitcoin in the message text.")
        return

    if sd:
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
            message = 'Current Value for {}: {} Mean Value: {} (Range: {} - {}) (Last Refreshed: {})'.format(
                stock_symbol, current_value, sd.mean_value, sd.date_range[
                    0], sd.date_range[1], sd.last_refreshed
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

                payload = io.BytesIO(json.dumps(
                    graph_payload, ensure_ascii=False).encode('utf8'))
                res = boto3.client('lambda').invoke(
                    FunctionName=GRAPH_FUNCTION_ARN,
                    Payload=payload,
                    InvocationType='Event'
                )
                logger.info("Invoked lambda: %s", str(res))
            else:
                logger.info("Sending Slack message to channel %s",
                            slack_event['channel'])
                _send_slack_message(slack_event['channel'], message)
            return

    _send_slack_message(slack_event['channel'], json.dumps(slack_event))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log-level', choices=('DEBUG', 'INFO', 'WARN',
                                                      'ERROR', 'TRACE'),
                        default='INFO')
    parser.add_argument('-s', '--stock-symbol', type=str)
    parser.add_argument('-b', '--bitcoin', action='store_true', default=False)
    parser.add_argument('-i', '--interval', type=str, default='')
    parser.add_argument('-o', '--output', type=str)

    options = parser.parse_args()

    logging.basicConfig(level=getattr(logging, options.log_level))

    sd = None
    interval_length, interval = _find_interval(options.interval)
    if options.bitcoin:
        sd = BtcData()

    if options.stock_symbol:
        sd = StockData(options.stock_symbol, interval, interval_length)

    logging.info("%s datapoints", len(sd.data))
    logging.debug(sd.data)
    logging.debug(sd.metadata)
    logging.info("Current Value:%s Mean Value:%s (%s - %s) Last Refreshed:%s",
                 sd.current_value,
                 sd.mean_value,
                 sd.date_range[0],
                 sd.date_range[1],
                 sd.last_refreshed)

    if options.output:
        with open(options.output, 'w') as output_file:
            json.dump(sd.graph(), output_file, indent=2)
