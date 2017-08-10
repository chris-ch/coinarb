import argparse
import logging

import pandas
import tenacity
import time
from btfxwss import BtfxWss
import bitfinex

from arbitrage import scan_arbitrage_opportunities


def parse_symbols(pairs):
    symbols = set()
    for pair in pairs:
        base_symbol = pair[:len(pair) // 2]
        quote_symbol = pair[len(pair) // 2:]
        symbols.add((base_symbol, quote_symbol))

    return symbols


@tenacity.retry(wait=tenacity.wait_fixed(1), stop=tenacity.stop_after_attempt(1))
def main(args):
    bitfinex_client = bitfinex.Client()
    pair_codes = bitfinex_client.symbols()
    pairs = parse_symbols(pair_codes)

    def order_book_l1(client):
        def wrapped(pair_code):
            result = client.order_book(pair_code)
            bids = pandas.DataFrame(result['bids']).rename(columns={'amount': 'volume'})[['timestamp', 'price', 'volume']]
            asks = pandas.DataFrame(result['asks']).rename(columns={'amount': 'volume'})[['timestamp', 'price', 'volume']]
            if bids is None or asks is None:
                return None, None

            return bids.iloc[0], asks.iloc[0]

        return wrapped

    scan_arbitrage_opportunities(pairs, order_book_l1(bitfinex_client))
    return


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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
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
    main(args)
