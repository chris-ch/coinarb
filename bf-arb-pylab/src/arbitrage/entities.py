import logging
from functools import total_ordering
from typing import List

import numpy
import itertools
from decimal import Decimal

import pandas


@total_ordering
class ArbitrageStrategy(object):
    """
    Models an arbitrage strategy.
    """
    def __init__(self, pair1, pair2, pair3):
        """

        :param pair1: CurrencyPair instance
        :param pair2: CurrencyPair instance
        :param pair3: CurrencyPair instance
        """
        self._pair1 = pair1
        self._pair2 = pair2
        self._pair3 = pair3
        self._quotes = {
            self._pair1: ForexQuote(None, None, None),
            self._pair2: ForexQuote(None, None, None),
            self._pair3: ForexQuote(None, None, None)
        }

    @property
    def pairs(self):
        return tuple(sorted([self._pair1, self._pair2, self._pair3]))

    def __repr__(self):
        return '[{},{},{}]'.format(*self.pairs)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return self.pairs == other.pairs

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return repr(self) <= repr(other)

    def update_quotes(self, order_book_callbak):
        """

        :param order_book_callbak: retrieves quote for given CurrencyPair instance
        :return:
        """
        common_pair, indirect_pair_1, indirect_pair_2 = self.pairs
        common_quote = order_book_callbak(common_pair)
        indirect_quote_1 = order_book_callbak(indirect_pair_1)
        indirect_quote_2 = order_book_callbak(indirect_pair_2)
        self._quotes = {
            common_pair: common_quote,
            indirect_pair_1: indirect_quote_1,
            indirect_pair_2: indirect_quote_2
        }

    @property
    def quotes(self):
        """

        :return:
        """
        return self._quotes

    @property
    def quotes_valid(self):
        """

        :return:
        """
        is_valid = True
        for pair, quote in self.quotes.items():
            is_valid = is_valid and quote.is_complete()

        return is_valid

    def find_opportunities(self, illimited_volume, skip_capped=True):
        """

        :param illimited_volume: emulates infinite liquidity
        :param skip_capped:
        :return:
        """
        logging.info('trying strategy'.format(self))
        opportunities = list()
        for pair1, pair2, pair3 in itertools.permutations(self.pairs):
            currency_initial = pair1.quote
            initial_quote = self.quotes[pair1]
            if currency_initial in pair2.assets:
                next_pair = pair2
                next_quote = self.quotes[pair2]
                final_pair = pair3
                final_quote = self.quotes[pair3]

            else:
                next_pair = pair3
                next_quote = self.quotes[pair3]
                final_pair = pair2
                final_quote = self.quotes[pair2]

            if next_pair.base != currency_initial:
                currency_next = next_pair.base

            else:
                currency_next = next_pair.quote

            balance_initial, trade_initial = pair1.buy_currency(currency_initial, 1, initial_quote, illimited_volume)
            balance_next, trade_next = next_pair.sell_currency(currency_initial, balance_initial[currency_initial],
                                                               next_quote, illimited_volume)
            balance_final, trade_final = final_pair.sell_currency(currency_next, balance_next[currency_next],
                                                                  final_quote, illimited_volume)

            balance1_series = pandas.Series(balance_initial, name='initial')
            balance2_series = pandas.Series(balance_next, name='next')
            balance3_series = pandas.Series(balance_final, name='final')
            balances = pandas.concat([balance1_series, balance2_series, balance3_series], axis=1)
            trades_df = pandas.DataFrame([trade_initial, trade_next, trade_final])
            if not skip_capped or trades_df['capped'].count() == 0:
                logging.info('adding new opportunity:\n{}'.format(trades_df))
                logging.info('resulting balances:\n{}'.format(balances.sum(axis=1)))
                opportunities.append((trades_df, balances.sum(axis=1)))

            else:
                logging.info('no opportunity')

        return opportunities


@total_ordering
class CurrencyPair(object):
    """
    Models a currency pair.
    """
    def __init__(self, base_currency_code, quote_currency_code):
        """
        The quotation EUR/USD 1.2500 means that one euro is exchanged for 1.2500 US dollars.
        Here, EUR is the base currency and USD is the quote currency(counter currency).
        :param base_currency_code: currency that is quoted
        :param quote_currency_code: currency that is used as the reference
        """
        self._base_currency_code = base_currency_code
        self._quote_currency_code = quote_currency_code

    def buy(self, quote, volume, illimited_volume=False):
        """
        Computes the balance after the buy has taken place.
        Example, provided volume is sufficient:
            quote = EUR/USD <1.15, 1.16>, volume = +1 ---> Balances: EUR = +1, USD = -1.16

        :param quote: ForexQuote instance
        :param volume:
        :param illimited_volume: emulates infinite liquidity
        :return:
        """
        price = quote.ask['price']
        if illimited_volume:
            allowed_volume = volume

        else:
            allowed_volume = min(volume, quote.ask['volume'])

        capped = numpy.NaN
        if allowed_volume < volume:
            capped = allowed_volume

        balance = {self.base: allowed_volume, self.quote: allowed_volume * price * -1}
        trade = {'direction': 'buy', 'pair': repr(self), 'quantity': allowed_volume, 'price': price, 'capped': capped}
        return balance, trade

    def sell(self, quote, volume, illimited_volume=False):
        """
        Computes the balance after the sell has taken place.
        Example, provided volume is sufficient:
            quote = EUR/USD <1.15, 1.16>, volume = 1 ---> Balances: EUR = -1, USD = +1.15

        :param quote: ForexQuote instance
        :param volume:
        :param illimited_volume: emulates infinite liquidity
        :return:
        """
        volume = abs(volume)
        price = quote.bid['price']
        if illimited_volume:
            allowed_volume = volume

        else:
            allowed_volume = min(volume, quote.bid['volume'])

        capped = numpy.NaN
        if allowed_volume < volume:
            capped = allowed_volume

        balance = {self.base: allowed_volume * -1, self.quote: allowed_volume * price}
        trade = {'direction': 'sell', 'pair': repr(self), 'quantity': allowed_volume * -1, 'price': price, 'capped': capped}
        return balance, trade

    def buy_currency(self, currency, volume, quote, illimited_volume=False):
        """

        :param currency: currency to buy
        :param volume: amount to buy denominated in currency
        :param quote: current quote (ForexQuote instance)
        :param illimited_volume: emulates infinite liquidity
        :return: resulting balance and performed trades (balance, performed_trade)
        """
        assert currency in self.assets, 'currency {} not in pair {}'.format(currency, self)
        logging.debug('buying {} {} using pair {}'.format(volume, currency, self))
        if currency == self.base:
            # Direct quotation
            balance, performed_trade = self.buy(quote, volume, illimited_volume)

        else:
            # Indirect quotation
            target_volume = Decimal(volume) / quote.bid['price']
            balance, performed_trade = self.sell(quote, target_volume, illimited_volume)

        return balance, performed_trade

    def sell_currency(self, currency, volume, quote, illimited_volume=False):
        """

        :param currency:
        :param volume: amount to buy denominated in currency
        :param quote: current quote (ForexQuote instance)
        :param illimited_volume: emulates infinite liquidity
        :return: resulting balance and performed trades (balance, performed_trade)
        """
        assert currency in self.assets, 'currency {} not in pair {}'.format(currency, self)
        logging.debug('selling {} {} using pair {}'.format(volume, currency, self))
        if currency == self.base:
            # Direct quotation
            balance, performed_trade = self.sell(quote, volume, illimited_volume)

        else:
            # Indirect quotation
            target_volume = Decimal(volume) / quote.ask['price']
            balance, performed_trade = self.buy(quote, target_volume, illimited_volume)

        return balance, performed_trade

    @property
    def assets(self):
        return {self._base_currency_code, self._quote_currency_code}

    @property
    def quote(self):
        return self._quote_currency_code

    @property
    def base(self):
        return self._base_currency_code

    def to_direct(self, separator='/'):
        return '{}{}{}'.format(self._base_currency_code, separator, self._quote_currency_code)

    def to_indirect(self, separator='/'):
        return '{}{}{}'.format(self._quote_currency_code, separator, self._base_currency_code)

    def __repr__(self):
        return '<{}/{}>'.format(self._base_currency_code, self._quote_currency_code)

    def __hash__(self):
        return hash(self._base_currency_code + self._quote_currency_code)

    def __eq__(self, other):
        return (self._base_currency_code == other._base_currency_code) and (self._quote_currency_code == other._quote_currency_code)

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return repr(self) <= repr(other)


class ForexQuote(object):
    """
    Models a forex quote.
    """
    def __init__(self, timestamp, bid, ask):
        self._timestamp = timestamp
        self._bid = bid
        self._ask = ask

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def bid(self):
        return self._bid

    @property
    def ask(self):
        return self._ask

    def is_complete(self):
        return self._bid is not None and self._ask is not None