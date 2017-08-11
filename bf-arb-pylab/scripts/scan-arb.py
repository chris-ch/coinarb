import argparse
import logging

import pandas
import tenacity
import time
from btfxwss import BtfxWss
import bitfinex
import requests_cache
from decimal import Decimal

from arbitrage import scan_arbitrage_opportunities, create_pair_from_indirect, create_pair_from_direct, ForexQuote


def parse_symbols_bitfinex(pairs):
    symbols = set()
    for pair_code in pairs:
        symbols.add(create_pair_from_indirect(pair_code))

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


#@tenacity.retry(wait=tenacity.wait_fixed(1), stop=tenacity.stop_after_attempt(1))
def main(args):
    bitfinex_client = bitfinex.Client()
    pair_codes = bitfinex_client.symbols()
    pairs = parse_symbols_bitfinex(pair_codes)

    def order_book_l1(client):
        def wrapped(pair):
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

    results = scan_arbitrage_opportunities(pairs, order_book_l1(bitfinex_client), illimited_volume=True)
    for trades, balances in results:
        print(trades)
        print(balances)

    return


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('update-nav-hist.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    parser = argparse.ArgumentParser(description='NAV history update.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')

    args = parser.parse_args()
    # DEBUGGING
    requests_cache.install_cache('demo_cache')

    main(args)
