import logging

import itertools

import numpy
import pandas
from decimal import Decimal


def create_pair_from_indirect(pair_code):
    return CurrencyPair(pair_code[len(pair_code) // 2:], pair_code[:len(pair_code) // 2])


def create_pair_from_direct(pair_code):
    return CurrencyPair(pair_code[:len(pair_code) // 2], pair_code[len(pair_code) // 2:])


class ForexQuote(object):

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


class CurrencyPair(object):
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


def calculate_arbitrage_opportunity(pair_1, quote_1, pair_2, quote_2, pair_3, quote_3, skip_capped=True, illimited_volume=False):
    """

    :param pair_1:
    :param quote_1:
    :param pair_2:
    :param quote_2:
    :param pair_3:
    :param quote_3:
    :param skip_capped:
    :param illimited_volume: emulates infinite liquidity
    :return: (trades, balances)
    """
    pairs = [pair_1, pair_2, pair_3]
    quotes = [quote_1, quote_2, quote_3]
    opportunities = list()
    for first, second, third in itertools.permutations([0, 1, 2]):
        currency_initial = pairs[first].quote
        initial_quote = quotes[first]
        if currency_initial in pairs[second].assets:
            next_pair = pairs[second]
            next_quote = quotes[second]
            final_pair = pairs[third]
            final_quote = quotes[third]

        else:
            next_pair = pairs[third]
            next_quote = quotes[third]
            final_pair = pairs[second]
            final_quote = quotes[second]

        if next_pair.base != currency_initial:
            currency_next = next_pair.base

        else:
            currency_next = next_pair.quote

        balance_initial, trade_initial = pairs[first].buy_currency(currency_initial, 1, initial_quote, illimited_volume)
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


def check_legs(common_leg, leg_pair1, leg_pair2, pairs, order_book_callbak, illimited_volume):
    """

    :param common_leg:
    :param leg_pair1:
    :param leg_pair2:
    :param pairs: set of CurrencyPair instances
    :param order_book_callbak: retrieves quote for given CurrencyPair instance
    :param illimited_volume: emulates infinite liquidity
    :return:
    """
    logging.debug('checking legs: {}, {}, {}'.format(common_leg, leg_pair1, leg_pair2))
    common_pair = CurrencyPair(leg_pair1, leg_pair2)
    indirect_pair_1 = CurrencyPair(leg_pair1, common_leg)
    indirect_pair_2 = CurrencyPair(leg_pair2, common_leg)

    if pairs.issuperset({common_pair, indirect_pair_1, indirect_pair_2}):
        logging.info('trying pair {} with {} and {}'.format(common_pair, indirect_pair_1, indirect_pair_2))
        common_quote = order_book_callbak(common_pair)
        indirect_quote_1 = order_book_callbak(indirect_pair_1)
        indirect_quote_2 = order_book_callbak(indirect_pair_2)
        complete_data = common_quote.is_complete() and indirect_quote_1.is_complete() and indirect_quote_2.is_complete()
        if complete_data:
            opportunities = calculate_arbitrage_opportunity(common_pair, common_quote,
                                                              indirect_pair_1, indirect_quote_1,
                                                              indirect_pair_2, indirect_quote_2, illimited_volume)
            return opportunities

    else:
        logging.debug('incompatible combination: {}, {}, {} not in {}'.format(common_pair, indirect_pair_1, indirect_pair_2, pairs))
        return None


def scan_arbitrage_opportunities(pairs, order_book_callbak, illimited_volume):
    """
    Scanning arbitrage opportunities over the indicated pairs.

    :param pairs: set of CurrencyPair instances
    :param order_book_callbak:
    :param illimited_volume: emulates infinite liquidity
    :return:
    """
    pairs = set(pairs)
    assets = set()
    for pair in pairs:
        assets.update(pair.assets)

    logging.info('available pairs ({}): {}'.format(len(pairs), pairs))
    logging.info('available assets ({}): {}'.format(len(assets), assets))
    results = list()
    itertools.permutations(())
    for common_leg, leg_pair1, leg_pair2 in itertools.permutations(assets, 3):
        opportunities = check_legs(common_leg, leg_pair1, leg_pair2, pairs, order_book_callbak, illimited_volume)
        if opportunities is not None and len(opportunities) > 0:
            results += opportunities

    return results
