import json
import asyncio
import websockets

WSS_BITFINEX_2 = 'wss://api2.bitfinex.com:3000/ws'


async def consumer_handler(pair):
    async with websockets.connect(WSS_BITFINEX_2) as websocket:
        subscription = json.dumps({
            'event': 'subscribe',
            'channel': 'book',
            'symbol': pair,
            'prec': 'P0',  # precision level
            'freq': 'F0',  # realtime
        })
        await websocket.send(subscription)
        event_info = json.loads(await websocket.recv())
        print(event_info['event'], event_info['version'])
        subscription_status = json.loads(await websocket.recv())
        subscribed_status = subscription_status['event']
        channel_id = subscription_status['chanId']
        print(subscribed_status, channel_id)

        # Order Book
        orderbook_snapshot = json.loads(await websocket.recv())
        print("> {}".format(orderbook_snapshot))
        while True:
            orderbook_update = json.loads(await websocket.recv())
            if orderbook_update[1] == 'hb':
                continue

            channel_id, price, count, amount = orderbook_update
            print("> {}, {}, {}".format(price, count, amount))


def main():
    asyncio.get_event_loop().run_until_complete(consumer_handler('BTCUSD'))


if __name__ == '__main__':
    main()