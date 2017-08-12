import unittest
import logging

import bitfinex
from decimal import Decimal

import requests_cache

from arbitrage import create_pair_from_indirect, ForexQuote, scan_arbitrage_opportunities


class FindArbitrageOpportuinitiesTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_arb(self):
        requests_cache.install_cache('test_set_1')
        bitfinex_client = bitfinex.Client()
        pair_codes = bitfinex_client.symbols()
        pairs = set()
        for pair_code in pair_codes:
            pairs.add(create_pair_from_indirect(pair_code))

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
        expected_strategy = {'<usd/bch>', '<eth/bch>', '<usd/eth>'}
        strategy_found = False
        for trades, balances in results:
            strategy = set(trades['pair'].tolist())
            if len(strategy.symmetric_difference(expected_strategy)) == 0 and abs(balances['eth']) > 1e-10:
                self.assertAlmostEqual(balances['eth'], -0.00575, places=6)
                self.assertAlmostEqual(balances['bch'], 0, places=6)
                self.assertAlmostEqual(balances['usd'], 0, places=6)
                strategy_found = True

        self.assertTrue(strategy_found, 'expected strategy not found')

        expected_trades = {'<usd/eos>': {'direction': 'sell', 'price': Decimal('1.7905000000'),
                                         'quantity': Decimal('-0.5585032113934655124266964535')},
                           '<eth/eos>': {'direction': 'buy', 'price': Decimal('0.0059800000'),
                                         'quantity': Decimal('167.2240802675585284280936455')},
                           '<usd/eth>': {'direction': 'buy', 'price': Decimal('299.5200000000'),
                                         'quantity': Decimal('0.5583068919189320527113169254')}}
        for trades, balances in results:
            current_trades = trades[['direction', 'pair', 'price', 'quantity']].set_index('pair').to_dict(
                orient='index')
            if current_trades == expected_trades:
                self.assertAlmostEqual(balances['usd'], -0.00019632, places=8)
                self.assertAlmostEqual(balances['eos'], 0, places=6)
                self.assertAlmostEqual(balances['eth'], 0, places=6)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
