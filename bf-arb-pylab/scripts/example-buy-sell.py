import argparse
import asyncio
import hashlib
import hmac
import json
import logging

import time

import sys

import os
import websockets
from websockets.py35.client import Connect

WSS_BITFINEX_2 = 'wss://api.bitfinex.com/ws/2'


async def authenticate(wsc: Connect, secret_key: str, api_key: str) -> object:
    """

    :param wsc:
    :param secret_key:
    :param api_key:
    :return:
    """
    nonce = str(int(time.time() * 1000000000))
    payload = 'AUTH' + nonce
    secret_bytes = bytes(secret_key, 'utf-8')
    h = hmac.new(secret_bytes, payload.encode('utf-8'), hashlib.sha384)
    signature = h.hexdigest()
    data = {'event': 'auth', 'apiKey': api_key, 'authPayload': payload,
            'authNonce': nonce, 'authSig': signature, 'filter': ['trading', 'algo']}
    await wsc.send(json.dumps(data))
    info = await wsc.recv()
    logging.info('server info received: {}'.format(info))
    capabilities = await wsc.recv()
    logging.info('capabilities: {}'.format(capabilities))


async def consumer_handler(secret_key, api_key):
    async with websockets.connect(WSS_BITFINEX_2) as websocket:
        await authenticate(websocket, secret_key, api_key)
        channel_account = {
            'event': 'subscribe',
            'channel': 'funding',
            'key': 'fUSD'
        }
        await websocket.send(json.dumps(channel_account))
        pong = await websocket.recv()
        print("< {}".format(pong))


def main(args):
    with open(args.secrets, 'r') as secret_json:
        secret_data = json.loads(secret_json.read())
        secret_key = secret_data['bitfinex.key.secret']
        api_key = secret_data['bitfinex.key.api']
        asyncio.get_event_loop().run_until_complete(consumer_handler(secret_key, api_key))


if __name__ == '__main__':
    script_name = sys.argv[0].split(os.path.sep)[-1].split('.')[0]
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', filename='{}.log'.format(script_name), filemode='w')
    parser = argparse.ArgumentParser(description='Example Bitfinex order execution.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')

    args = parser.parse_args()
    main(args)
