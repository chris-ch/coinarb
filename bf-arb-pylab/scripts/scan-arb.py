import argparse
import logging

import os

import sys
import tenacity
import time
from btfxwss import BtfxWss
import bitfinex
import requests_cache
from decimal import Decimal

from arbitrage import scan_arbitrage_opportunities, parse_pair_from_indirect, create_strategies, parse_strategy
from arbitrage.entities import ForexQuote, CurrencyPair


def parse_symbols_bitfinex(pairs):
    symbols = set()
    for pair_code in pairs:
        symbols.add(parse_pair_from_indirect(pair_code))

    return symbols


def wss():
    wss = BtfxWss()
    wss.start()
    time.sleep(1)  # give the client some prep time to set itself up.

    # Subscribe to some channels
    wss.subscribe_to_ticker('BTCUSD')
    wss.subscribe_to_order_book('BTCUSD')

    # Do something else
    t = time.time()
    while time.time() - t < 10:
        pass

    # Accessing data stored in BtfxWss:
    ticker_q = wss.tickers('BTCUSD')  # returns a Queue object for the pair.
    while not ticker_q.empty():
        print(ticker_q.get())

    # Unsubscribing from channels:
    wss.unsubscribe_from_ticker('BTCUSD')
    wss.unsubscribe_from_order_book('BTCUSD')

    # Shutting down the client:
    wss.stop()


class CurrencyConverter(object):
    def __init__(self, reference_currency, pair_codes, order_book_callback):
        self._reference_currency = reference_currency
        self._pair_codes = pair_codes
        self._order_book_callback = order_book_callback

    @property
    def reference_currency(self):
        return self._reference_currency

    @property
    def pairs(self):
        return self._pair_codes

    def exchange(self, currency, amount):
        if currency == self.reference_currency:
            return amount

        target_pair = None
        if currency + self.reference_currency in self.pairs:
            target_pair = CurrencyPair(self.reference_currency, currency)

        elif self.reference_currency + currency in self.pairs:
            target_pair = CurrencyPair(currency, self.reference_currency)

        if target_pair is None:
            raise LookupError('unable to find suitable pair for converting {} into {}'.format(currency, self.reference_currency))

        quote = self._order_book_callback(target_pair)
        return target_pair.convert(currency, amount, quote)


def main(args):
    if args.strategies:
        strategies_filename = os.path.abspath(args.strategies)
        logging.info('loading strategies from "{}"'.format(strategies_filename))
        with open(strategies_filename, 'r') as strategies_file:
            strategies = [parse_strategy(line) for line in strategies_file.readlines() if len(line.strip()) > 0]

        logging.info('loaded {} strategies'.format(len(strategies)))

    else:
        logging.info('loading strategies from standard input')
        strategies = list()
        for line in sys.stdin:
            if len(line.strip()) > 0:
                strategies.append(parse_strategy(line))

    def order_book_l1(client):
        """
        Creates a function returning a quote for a given currency pair.
        :param client:
        :return:
        """
        def wrapped(pair):
            """

            :param pair: CurrencyPair instance
            :return:
            """
            result = client.order_book(pair.to_indirect(separator=''))
            result_bid = result['bids'][0]
            result_ask = result['asks'][0]
            bid = dict()
            ask = dict()
            bid['price'] = round(Decimal(result_bid['price']), 10)
            ask['price'] = round(Decimal(result_ask['price']), 10)
            bid['volume'] = round(Decimal(result_bid['amount']), 10)
            ask['volume'] = round(Decimal(result_ask['amount']), 10)
            timestamp = result_bid['timestamp']
            return ForexQuote(timestamp, bid, ask)

        return wrapped

    bitfinex_client = bitfinex.Client()
    pair_codes = bitfinex_client.symbols()
    converter = CurrencyConverter('btc', pair_codes, order_book_l1(bitfinex_client))
    results = scan_arbitrage_opportunities(strategies, order_book_l1(bitfinex_client), illimited_volume=True)
    for trades, balances in results:
        bitcoin_amount = converter.exchange(balances['currency'], balances['remainder'])
        if bitcoin_amount > 0:
            logging.info('residual value: {}'.format(bitcoin_amount))
            logging.info('trades:\n{}'.format(trades))

    return


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('scan-arb.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    parser = argparse.ArgumentParser(description='Scanning arbitrage opportunities.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')
    parser.add_argument('--strategies', type=str, help='list of strategies')

    args = parser.parse_args()
    # DEBUGGING
    requests_cache.install_cache('demo_cache')

    main(args)
