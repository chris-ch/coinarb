import unittest
import logging

import bitfinex
from decimal import Decimal
from datetime import datetime

import requests_cache

from arbitrage import parse_pair_from_indirect, create_strategies, parse_currency_pair, parse_strategy
from arbitrage.entities import ForexQuote, ArbitrageStrategy, CurrencyPair, CurrencyConverter


class FindArbitrageOpportunitiesTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_find_strategies(self):
        requests_cache.install_cache('test_set_1')
        bitfinex_client = bitfinex.Client()
        pair_codes = bitfinex_client.symbols()
        pairs = set()
        for pair_code in pair_codes:
            pairs.add(parse_pair_from_indirect(pair_code))

        strategies = set(create_strategies(pairs))
        expected_strategies = {
            ArbitrageStrategy(parse_currency_pair(pair1), parse_currency_pair(pair2), parse_currency_pair(pair3)) for
            pair1, pair2, pair3 in
            [('<eth/bch>', '<usd/bch>', '<usd/eth>'), ('<btc/rrt>', '<usd/btc>', '<usd/rrt>'),
             ('<btc/etc>', '<usd/btc>', '<usd/etc>'), ('<btc/eth>', '<btc/san>', '<eth/san>'),
             ('<btc/xrp>', '<usd/btc>', '<usd/xrp>'), ('<btc/eth>', '<btc/omg>', '<eth/omg>'),
             ('<btc/bcc>', '<usd/bcc>', '<usd/btc>'), ('<btc/omg>', '<usd/btc>', '<usd/omg>'),
             ('<eth/eos>', '<usd/eos>', '<usd/eth>'), ('<btc/iot>', '<usd/btc>', '<usd/iot>'),
             ('<eth/omg>', '<usd/eth>', '<usd/omg>'), ('<btc/bch>', '<usd/bch>', '<usd/btc>'),
             ('<btc/dsh>', '<usd/btc>', '<usd/dsh>'), ('<btc/ltc>', '<usd/btc>', '<usd/ltc>'),
             ('<btc/xmr>', '<usd/btc>', '<usd/xmr>'), ('<btc/bcu>', '<usd/bcu>', '<usd/btc>'),
             ('<eth/iot>', '<usd/eth>', '<usd/iot>'), ('<btc/eth>', '<usd/btc>', '<usd/eth>'),
             ('<eth/san>', '<usd/eth>', '<usd/san>'), ('<btc/san>', '<usd/btc>', '<usd/san>'),
             ('<btc/eth>', '<btc/iot>', '<eth/iot>'), ('<btc/eos>', '<usd/btc>', '<usd/eos>'),
             ('<btc/bch>', '<btc/eth>', '<eth/bch>'), ('<btc/eos>', '<btc/eth>', '<eth/eos>'),
             ('<btc/zec>', '<usd/btc>', '<usd/zec>')]}
        self.assertSetEqual(set(strategies), expected_strategies)

    def test_parse_strategy(self):
        input = '[<btc/eth>,<usd/btc>,<usd/eth>]'
        strategy = parse_strategy(input)
        self.assertEqual(strategy, ArbitrageStrategy(CurrencyPair('btc', 'eth'), CurrencyPair('usd', 'btc'),
                                                     CurrencyPair('usd', 'eth')))

    def test_converter(self):
        def quote_loader(pair):
            bid = {'price': Decimal('0.66'), 'volume': Decimal(100)}
            ask = {'price': Decimal('0.67'), 'volume': Decimal(100)}
            return ForexQuote(datetime(2017, 1, 1), bid, ask)

        converter = CurrencyConverter(('usd', 'gbp'), quote_loader)
        self.assertEqual(converter.buy('gbp', Decimal('0.66')), Decimal(-1))  # 0.66 GBP cost 1 USD
        self.assertEqual(converter.sell('gbp', Decimal('0.67')), Decimal(1))  # 0.67 GBP needed for receiving 1 USD
        self.assertAlmostEqual(converter.buy('gbp', Decimal('1.')), Decimal(-1.52), places=2)  # 1 GBP costs 1.52 USD

    def test_arbitrage(self):
        def quote_loader(pair):
            if pair == '':
                bid = {'price': Decimal('0.66'), 'volume': Decimal(100)}
                ask = {'price': Decimal('0.67'), 'volume': Decimal(100)}
            elif pair == '':
                bid = {'price': Decimal('0.66'), 'volume': Decimal(100)}
                ask = {'price': Decimal('0.67'), 'volume': Decimal(100)}
            elif pair == '':
                bid = {'price': Decimal('0.66'), 'volume': Decimal(100)}
                ask = {'price': Decimal('0.67'), 'volume': Decimal(100)}
            else:
                raise NotImplementedError('illegal pair: {}'.format(pair))

            return ForexQuote(datetime(2017, 1, 1), bid, ask)

        input = '[<btc/eth>,<usd/btc>,<usd/eth>]'
        strategy = parse_strategy(input)
        strategy.update_quotes(quote_loader)
        if strategy.quotes_valid:
            result = strategy.find_opportunities(illimited_volume=True)
            for trades, balances in result:
                market = ('btc', balances['currency'])
                converter = CurrencyConverter(market, quote_loader)
                bitcoin_amount = converter.exchange(balances['currency'], balances['remainder'])
                if bitcoin_amount > 0:
                    logging.info('residual value: {}'.format(bitcoin_amount))
                    logging.info('trades:\n{}'.format(trades))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
