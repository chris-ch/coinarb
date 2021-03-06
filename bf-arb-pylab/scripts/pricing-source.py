import argparse
import logging
from collections import defaultdict

from decimal import Decimal

from typing import Callable, Any

import os

import sys

from arbitrage.entities import OrderBook, QuoteEncoder

import json
import asyncio
import websockets

WSS_BITFINEX_2 = 'wss://api2.bitfinex.com:3000/ws'


async def consumer_handler(pairs, notify_update_func: Callable[[str, OrderBook], Any]):
    """

    :param pairs:
    :param notify_update_func:
    :return:
    """
    channel_pair_mapping = dict()
    orderbooks = dict()
    async with websockets.connect(WSS_BITFINEX_2) as websocket:
        for pair in pairs:
            subscription = json.dumps({
                'event': 'subscribe',
                'channel': 'book',
                'symbol': pair,
                'prec': 'P0',  # precision level
                'freq': 'F0',  # realtime
            })
            await websocket.send(subscription)

        while True:
            response = json.loads(await websocket.recv(), parse_float=Decimal)
            if hasattr(response, 'keys'):
                if 'version' in response.keys():
                    logging.info('event: {} {}'.format(response['event'], response['version']))

                else:
                    subscription_status = response['event']
                    channel_id = response['chanId']
                    pair = response['pair']
                    if subscription_status != 'subscribed':
                        message = 'failed to subscribe: {}'.format(pair)
                        logging.error(message)
                        raise RuntimeError(message)

                    channel_pair_mapping[channel_id] = pair
                    logging.info('successfully subscribed: {}'.format(pair))

            else:
                if len(response) == 2:
                    if response[1] == 'hb':
                        continue

                    channel_id = response[0]
                    pair = channel_pair_mapping[channel_id]
                    if pair not in orderbooks:
                        orderbooks[pair] = OrderBook(pair=pair, source='bitfinex')

                    orderbooks[pair].load_snapshot(response)
                    logging.info('> loaded snapshot order book {}'.format(orderbooks[pair]))
                    notify_update_func(pair, orderbooks[pair])

                elif len(response) == 4:
                    # Order Book update
                    channel_id, price, count, amount = response
                    pair = channel_pair_mapping[channel_id]
                    if count > 0:
                        if amount > 0:
                            updated = orderbooks[pair].update_bid(price, amount)

                        else:
                            updated = orderbooks[pair].update_ask(price, amount)

                    else:
                        if amount == 1:
                            updated = orderbooks[pair].remove_bid(price)

                        else:
                            updated = orderbooks[pair].remove_ask(price)

                    if updated:
                        notify_update_func(pair, orderbooks[pair])

                else:
                    logging.error('unexpected response: {}'.format(response))


def main(args):
    unbuffered_stdout = os.fdopen(sys.stdout.fileno(), 'wb', 0)
    pairs = [''.join(pair.upper().split('/')) for pair in args.bitfinex.split(',')]
    last_quote = defaultdict(lambda: None)

    def notify_update(pair, order_book):
        level_one_quote = order_book.level_one()
        if last_quote[pair] is None or level_one_quote.bid != last_quote[pair].bid or level_one_quote.ask != last_quote[pair].ask:
            last_quote[pair] = level_one_quote
            level_one_dict = level_one_quote.to_dict()
            level_one_dict['pair'] = pair[:len(pair) // 2] + '/' + pair[len(pair) // 2:]
            json_line = json.dumps(level_one_dict, cls=QuoteEncoder)
            logging.debug('{}: updated book {}'.format(pair, level_one_quote))
            unbuffered_stdout.write(json_line.encode('utf-8'))
            unbuffered_stdout.write('\n'.encode('utf-8'))

    asyncio.get_event_loop().run_until_complete(consumer_handler(pairs, notify_update))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', filename='pricing-source.log', filemode='w')
    parser = argparse.ArgumentParser(description='Sending quotes for subscribed prices.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')
    parser.add_argument('--bitfinex', type=str, help='list of pairs to subscribe to on bitfinex (for example: btcusd,eosbtc,eosusd)')

    args = parser.parse_args()
    main(args)
