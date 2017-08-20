import argparse
import logging

from multiprocessing import Process
import time
from datetime  import datetime
from decimal import Decimal

from collections import defaultdict

from arbitrage.entities import ForexQuote, CurrencyPair, PriceVolume

import json
import asyncio
import websockets

WSS_BITFINEX_2 = 'wss://api2.bitfinex.com:3000/ws'


async def consumer_handler(pairs):
    channel_pair_mapping = dict()
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
            response = json.loads(await websocket.recv())
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

                    # Order Book snapshot
                    print("> {}".format(response))

                else:
                    # Order Book update
                    channel_id, price, count, amount = response
                    print("> {}, {}, {}".format(channel_pair_mapping[channel_id], price, count, amount))


def main(args):
    pairs = [''.join(pair.upper().split('/')) for pair in args.bitfinex.split(',')]
    asyncio.get_event_loop().run_until_complete(consumer_handler(pairs))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('pricing-source.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    parser = argparse.ArgumentParser(description='Sending quotes for subscribed prices.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')
    parser.add_argument('--bitfinex', type=str, help='list of pairs to subscribe to on bitfinex (for example: eth/btc,usd/eth,eos/etc)')

    args = parser.parse_args()
    main(args)