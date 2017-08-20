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

from arbitrage import scan_arbitrage_opportunities, parse_pair_from_indirect, parse_strategy
from arbitrage.entities import ForexQuote, CurrencyPair, PriceVolume


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


def main(args):
    if args.strategy:
        strategy = parse_strategy(args.strategy, indirect_mode=True)

    else:
        logging.info('loading strategy from standard input')
        strategy = sys.stdin.readline()

    def order_book_l1(client: bitfinex.Client):
        """
        Creates a function returning a quote for a given currency pair.
        :param client:
        :return:
        """
        def wrapped(pair: CurrencyPair):
            """

            :param pair: CurrencyPair instance
            :return:
            """
            pair_code = pair.to_direct(separator='')
            result = client.order_book(pair_code)
            result_bid = result['bids'][0]
            result_ask = result['asks'][0]
            bid_price = round(Decimal(result_bid['price']), 10)
            ask_price = round(Decimal(result_ask['price']), 10)
            bid_volume = round(Decimal(result_bid['amount']), 10)
            ask_volume = round(Decimal(result_ask['amount']), 10)
            timestamp = result_bid['timestamp']
            return ForexQuote(timestamp, PriceVolume(bid_price, bid_volume), PriceVolume(ask_price, ask_volume), source='bitfinex')

        return wrapped

    bitfinex_client = bitfinex.Client()
    scan_arbitrage_opportunities(strategy, order_book_l1, bitfinex_client, illimited_volume=True)


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
    parser.add_argument('--strategy', type=str, help='strategy as a formatted string (for example: eth/btc,btc/usd,eth/usd')

    args = parser.parse_args()
    # DEBUGGING
    requests_cache.install_cache('demo_cache')

    main(args)
