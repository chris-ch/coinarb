import json
import unittest
import logging

import bitfinex
from decimal import Decimal
from datetime import datetime

import itertools

import requests_cache

from arbitrage import parse_pair_from_indirect, create_strategies, parse_currency_pair, parse_strategy
from arbitrage.entities import ForexQuote, ArbitrageStrategy, CurrencyPair, CurrencyConverter, PriceVolume, OrderBook


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
            bid = PriceVolume(Decimal('0.66'), Decimal(100))
            ask = PriceVolume(Decimal('0.67'), Decimal(100))
            return ForexQuote(datetime(2017, 1, 1), bid, ask)

        converter = CurrencyConverter(('usd', 'gbp'), quote_loader)
        self.assertEqual(converter.buy('gbp', Decimal('0.66')), Decimal(-1))  # 0.66 GBP cost 1 USD
        self.assertEqual(converter.sell('gbp', Decimal('0.67')), Decimal(1))  # 0.67 GBP needed for receiving 1 USD
        self.assertAlmostEqual(converter.buy('gbp', Decimal('1.')), Decimal(-1.52), places=2)  # 1 GBP costs 1.52 USD

    def test_arbitrage_ordering(self):
        currencies = ('A', 'B', 'C')
        pairs = list()
        for currency1, currency2 in itertools.permutations(currencies, 2):
            pairs.append('{}/{}'.format(currency1, currency2))

        for pair1, pair2, pair3 in itertools.permutations(pairs, 3):
            if set(pair1.split('/')) == set(pair2.split('/')):
                continue

            if set(pair1.split('/')) == set(pair3.split('/')):
                continue

            if set(pair2.split('/')) == set(pair3.split('/')):
                continue

            input = '<{}>,<{}>,<{}>'.format(pair1, pair2, pair3)
            strategy = parse_strategy(input)
            self.assertEqual(strategy.indirect_pairs[0].quote, strategy.indirect_pairs[1].base)
            self.assertNotEqual(strategy.direct_pair.base, strategy.indirect_pairs[0].quote)
            self.assertNotEqual(strategy.direct_pair.quote, strategy.indirect_pairs[1].base)

    def test_pair_trading(self):
        """
        EUR.CHF	1.14	1.15
        CHF.USD	1.04	1.05
        EUR.USD	1.19	1.2

        Indirect	  EUR.CHF       CHF.USD         Direct              Total
        EUR	            -1000                          EUR   1000           0
        CHF              1140         -1140					                0
        USD	                0        1185.6            USD  -1180        5.60
        """
        pair_eur_chf = CurrencyPair('eur', 'chf')
        pair_chf_usd = CurrencyPair('chf', 'usd')
        pair_eur_usd = CurrencyPair('eur', 'usd')
        quote_eur_chf = ForexQuote(bid=PriceVolume(Decimal('1.14'), 10000), ask=PriceVolume(Decimal('1.15'), 10000))
        quote_chf_usd = ForexQuote(bid=PriceVolume(Decimal('1.04'), 10000), ask=PriceVolume(Decimal('1.05'), 10000))
        quote_eur_usd = ForexQuote(bid=PriceVolume(Decimal('1.17'), 10000), ask=PriceVolume(Decimal('1.18'), 10000))

        balance_sell_eur_chf, trade_sell_eur_chf = pair_eur_chf.sell(quote_eur_chf, Decimal(1000),
                                                                     illimited_volume=True)
        balance_sell_chf_usd, trade_sell_chf_usd = pair_chf_usd.sell(quote_chf_usd, balance_sell_eur_chf['chf'],
                                                                     illimited_volume=True)
        balance_buy_eur_usd, trade_buy_eur_usd = pair_eur_usd.buy_currency(pair_eur_chf.base, Decimal(1000),
                                                                           quote_eur_usd, illimited_volume=True)

        self.assertEqual(trade_sell_eur_chf.direction, 'sell')
        self.assertEqual(trade_sell_eur_chf.quantity, -1000)
        self.assertEqual(trade_sell_chf_usd.direction, 'sell')
        self.assertEqual(trade_sell_chf_usd.quantity, -1140)
        self.assertEqual(trade_buy_eur_usd.direction, 'buy')
        self.assertEqual(trade_buy_eur_usd.quantity, 1000)
        self.assertAlmostEqual(trade_sell_eur_chf.price, Decimal('1.14'), places=18)
        self.assertAlmostEqual(trade_sell_chf_usd.price, Decimal('1.04'), places=18)
        self.assertAlmostEqual(trade_buy_eur_usd.price, Decimal('1.18'), places=18)

        self.assertAlmostEqual(balance_sell_eur_chf['eur'], Decimal('-1000'), places=18)
        self.assertAlmostEqual(balance_sell_eur_chf['chf'], Decimal('1140'), places=18)
        self.assertAlmostEqual(balance_sell_chf_usd['chf'], Decimal('-1140'), places=18)
        self.assertAlmostEqual(balance_sell_chf_usd['usd'], Decimal('1185.6'), places=18)
        self.assertAlmostEqual(balance_buy_eur_usd['eur'], Decimal('1000'), places=18)
        self.assertAlmostEqual(balance_buy_eur_usd['usd'], Decimal('-1180'), places=18)

    def test_arbitrage_1(self):
        def quote_loader(pair):
            if pair == CurrencyPair('eur', 'chf'):
                bid = PriceVolume(Decimal('1.14'), Decimal(100))
                ask = PriceVolume(Decimal('1.15'), Decimal(100))
            elif pair == CurrencyPair('chf', 'usd'):
                bid = PriceVolume(Decimal('1.04'), Decimal(100))
                ask = PriceVolume(Decimal('1.05'), Decimal(100))
            elif pair == CurrencyPair('usd', 'eur'):
                bid = PriceVolume(Decimal('0.845'), Decimal(100))
                ask = PriceVolume(Decimal('0.855'), Decimal(100))
            else:
                raise NotImplementedError('illegal pair: {}'.format(pair))

            return ForexQuote(datetime(2017, 1, 1), bid, ask)

        input = '<eur/chf>,<chf/usd>,<usd/eur>'
        strategy = parse_strategy(input)
        self.assertEqual(strategy.direct_pair, CurrencyPair('usd', 'eur'))
        self.assertEqual(strategy.indirect_pairs[0], CurrencyPair('eur', 'chf'))
        self.assertEqual(strategy.indirect_pairs[1], CurrencyPair('chf', 'usd'))
        strategy.update_quotes(quote_loader)
        self.assertTrue(strategy.quotes_valid)
        balances, trades = strategy.apply_arbitrage(illimited_volume=True)
        self.assertAlmostEqual(balances['next'].loc['usd'], Decimal('1.185600'), places=6)
        self.assertAlmostEqual(balances['final'].loc['usd'], Decimal('-1.183432'), places=6)

    def test_arbitrage_2(self):
        def quote_loader(pair):
            if pair == CurrencyPair('eur', 'chf'):
                bid = PriceVolume(Decimal('1.14'), Decimal(100))
                ask = PriceVolume(Decimal('1.15'), Decimal(100))
            elif pair == CurrencyPair('chf', 'usd'):
                bid = PriceVolume(Decimal('1.04'), Decimal(100))
                ask = PriceVolume(Decimal('1.05'), Decimal(100))
            elif pair == CurrencyPair('eur', 'usd'):
                bid = PriceVolume(Decimal('1.18'), Decimal(100))
                ask = PriceVolume(Decimal('1.19'), Decimal(100))
            else:
                raise NotImplementedError('illegal pair: {}'.format(pair))

            return ForexQuote(datetime(2017, 1, 1), bid, ask)

        input = '[<eur/chf>,<chf/usd>,<eur/usd>]'
        strategy = parse_strategy(input)
        self.assertEqual(strategy.direct_pair, CurrencyPair('eur', 'usd'))
        self.assertEqual(strategy.indirect_pairs[0], CurrencyPair('eur', 'chf'))
        self.assertEqual(strategy.indirect_pairs[1], CurrencyPair('chf', 'usd'))
        strategy.update_quotes(quote_loader)
        self.assertTrue(strategy.quotes_valid)
        balances, trades = strategy.apply_arbitrage(illimited_volume=True)
        total = balances.sum(axis=1)
        self.assertAlmostEqual(total['usd'], -0.0044, places=4)
        self.assertAlmostEqual(total['chf'], 0, places=4)
        self.assertAlmostEqual(total['eur'], 0, places=4)

    def test_orderbook(self):
        snapshot = ['75', [['0.0003346', '4', '37.62485165'], ['0.00033459', '1', '8730.72318672'], ['0.000333', '1', '350'],
                         ['0.00033198', '2', '0.2'], ['0.00033197', '1', '0.1'], ['0.00033196', '1', '0.1'], ['0.00033176', '1', '0.1'],
                         ['0.00033173', '1', '0.1'], ['0.0003312', '1', '500'], ['0.00033101', '1', '86.744'], ['0.000331', '1', '6451.4199'],
                         ['0.00033023', '1', '740.87686'], ['0.00033011', '1', '741.14618'], ['0.00033', '2', '139.46531923'],
                         ['0.00032511', '1', '2887.53883609'], ['0.0003251', '2', '4778.30604615'], ['0.00032503', '1', '606.92814785'],
                         ['0.000325', '3', '94'], ['0.00032371', '1', '84.36364058'], ['0.0003237', '1', '12.3571205'],
                         ['0.00032369', '1', '17.4'], ['0.000323', '1', '1530'], ['0.000322', '1', '1'], ['0.000321', '2', '1050'],
                         ['0.00032021', '1', '6.05'], ['0.00033529', '1', '-98.41876716'], ['0.00033537', '1', '-153.46272053'],
                         ['0.00033548', '1', '-153.46272053'], ['0.0003356', '1', '-249.9'], ['0.00033588', '8', '-58.07091549'],
                         ['0.00033602', '1', '-2846.53727947'], ['0.00033664', '1', '-0.1'], ['0.00033665', '1', '-0.1'],
                         ['0.00033666', '1', '-0.1'], ['0.00033667', '1', '-0.1'], ['0.0003367', '5', '-7776.02600001'],
                         ['0.00033673', '1', '-0.1'], ['0.00033674', '1', '-0.1'], ['0.00033679', '1', '-930.2741439'],
                         ['0.0003368', '1', '-5000'], ['0.00033682', '2', '-0.2'], ['0.00033683', '2', '-0.2'],
                         ['0.00033887', '1', '-12.46962851'], ['0.0003396', '1', '-20'], ['0.00033969', '1', '-729.26098'],
                         ['0.0003397', '1', '-4568.4'], ['0.0003408', '1', '-5721.8059'], ['0.0003409', '1', '-50000'],
                         ['0.00034095', '1', '-0.15968'], ['0.00034149', '1', '-5.09291225']]]
        orderbook = OrderBook()
        orderbook.load_snapshot(snapshot)
        self.assertEqual(orderbook.quotes_bid[0]['price'], Decimal('0.0003346'))
        self.assertEqual(orderbook.quotes_bid[-1]['price'], Decimal('0.00032021'))
        self.assertEqual(orderbook.quotes_ask[0]['price'], Decimal('0.00033529'))
        self.assertEqual(orderbook.quotes_ask[-1]['price'], Decimal('0.00034149'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
