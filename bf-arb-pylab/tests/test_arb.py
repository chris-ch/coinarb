import unittest
import logging

import bitfinex
from decimal import Decimal
from datetime import datetime

import itertools
import requests_cache

from arbitrage import parse_pair_from_indirect, create_strategies, parse_currency_pair, parse_strategy
from arbitrage.entities import ForexQuote, ArbitrageStrategy, CurrencyPair, CurrencyConverter, PriceVolume


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

            input = '[<{}>,<{}>,<{}>]'.format(pair1, pair2, pair3)
            strategy = parse_strategy(input)
            self.assertEqual(strategy.indirect_pairs[0].quote, strategy.indirect_pairs[1].base)
            self.assertNotEqual(strategy.direct_pair.base, strategy.indirect_pairs[0].quote)
            self.assertNotEqual(strategy.direct_pair.quote, strategy.indirect_pairs[1].base)

    def test_pair_trading(self):
        pair_eur_chf = CurrencyPair('eur', 'chf')
        quote_eur_chf = ForexQuote(bid=PriceVolume(1.14, 100), ask=PriceVolume(1.15, 100))
        balance_buy_eur_chf, trade_buy_eur_chf = pair_eur_chf.buy(quote_eur_chf, 1, illimited_volume=True)
        self.assertEqual(balance_buy_eur_chf['eur'], 1)
        self.assertAlmostEqual(balance_buy_eur_chf['chf'], -1.15, places=18)
        self.assertEqual(trade_buy_eur_chf.direction, 'buy')
        self.assertEqual(trade_buy_eur_chf.quantity, 1)
        self.assertAlmostEqual(trade_buy_eur_chf.price, 1.15, places=18)

        balance_sell_eur_chf, trade_sell_eur_chf = pair_eur_chf.sell(quote_eur_chf, 1, illimited_volume=True)
        self.assertEqual(balance_sell_eur_chf['eur'], -1)
        self.assertAlmostEqual(balance_sell_eur_chf['chf'], 1.14, places=18)
        self.assertEqual(trade_sell_eur_chf.direction, 'sell')
        self.assertEqual(trade_sell_eur_chf.quantity, -1)
        self.assertAlmostEqual(trade_sell_eur_chf.price, 1.14, places=18)

        pair_chf_usd = CurrencyPair('chf', 'usd')
        quote_chf_usd = ForexQuote(bid=PriceVolume(Decimal('1.04'), 100), ask=PriceVolume(Decimal('1.05'), 100))
        balance_buy_chf_usd, trade_buy_chf_usd = pair_chf_usd.buy(quote_chf_usd, 1, illimited_volume=True)
        self.assertEqual(balance_buy_chf_usd['chf'], 1)
        self.assertAlmostEqual(balance_buy_chf_usd['usd'], Decimal('-1.05'), places=18)
        self.assertEqual(trade_buy_chf_usd.direction, 'buy')
        self.assertEqual(trade_buy_chf_usd.quantity, 1)
        self.assertAlmostEqual(trade_buy_chf_usd.price, Decimal('1.05'), places=18)

        balance_buy_chf_usd, trade_buy_chf_usd = pair_chf_usd.buy(quote_chf_usd, Decimal('1.15'), illimited_volume=True)
        self.assertAlmostEqual(balance_buy_chf_usd['chf'], Decimal('1.15'), places=18)
        self.assertAlmostEqual(balance_buy_chf_usd['usd'], Decimal('-1.2075'), places=18)
        self.assertEqual(trade_buy_chf_usd.direction, 'buy')
        self.assertEqual(trade_buy_chf_usd.quantity, Decimal('1.15'))
        self.assertAlmostEqual(trade_buy_chf_usd.price, Decimal('1.05'), places=18)

    def test_arbitrage(self):
        def quote_loader(pair):
            if pair == CurrencyPair('eur', 'chf'):
                bid = PriceVolume(Decimal('1.14'), Decimal(100))
                ask = PriceVolume(Decimal('1.15'), Decimal(100))
            elif pair == CurrencyPair('chf', 'usd'):
                bid = PriceVolume(Decimal('1.04'), Decimal(100))
                ask = PriceVolume(Decimal('1.05'), Decimal(100))
            elif pair == CurrencyPair('eur', 'usd'):
                bid = PriceVolume(Decimal('1.19'), Decimal(100))
                ask = PriceVolume(Decimal('1.20'), Decimal(100))
            # remaining balances
            elif pair == CurrencyPair('usd', 'eur'):
                bid = PriceVolume(Decimal('0.835'), Decimal(100))
                ask = PriceVolume(Decimal('0.840'), Decimal(100))
            elif pair == CurrencyPair('usd', 'chf'):
                bid = PriceVolume(Decimal('0.955'), Decimal(100))
                ask = PriceVolume( Decimal('0.960'), Decimal(100))
            else:
                raise NotImplementedError('illegal pair: {}'.format(pair))

            return ForexQuote(datetime(2017, 1, 1), bid, ask)

        input = '[<eur/chf>,<chf/usd>,<usd/eur>]'
        strategy = parse_strategy(input)
        self.assertEqual(strategy.direct_pair, CurrencyPair('usd', 'eur'))
        self.assertEqual(strategy.indirect_pairs[0], CurrencyPair('eur', 'chf'))
        self.assertEqual(strategy.indirect_pairs[1], CurrencyPair('chf', 'usd'))
        strategy.update_quotes(quote_loader)
        self.assertTrue(strategy.quotes_valid)
        trades, balances = strategy.apply_arbitrage(illimited_volume=True)
        print('--------------------')
        print(trades)
        print(balances)
        """
        EUR.CHF	1.14	1.15					
        CHF.USD	1.04	1.05					
        EUR.USD	1.19	1.2					
                                    
        EUR     CHF     USD					
        1000	1140	1185.6	Indirect				
        1000            1190    Direct				
                                    
        Indirect	  EUR.CHF       CHF.USD         Direct              Total
        EUR	            -1000                          EUR   1000           0
        CHF              1140         -1140					                0
        USD	                0        1185.6            USD  -1190        -4.4
        """
        #market = ('usd', balances['currency'])
        #converter = CurrencyConverter(market, quote_loader)
        #remaining_amount = converter.exchange(balances['currency'], balances['remainder'])
        #if remaining_amount > 0:
        #    logging.info('residual value: {}'.format(remaining_amount))
        #    logging.info('trades:\n{}'.format(trades))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
