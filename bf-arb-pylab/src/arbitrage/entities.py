import logging
from functools import total_ordering
from typing import Tuple, NamedTuple, Dict, Callable, Set, Any, List

import numpy
import itertools
from decimal import Decimal
from datetime import datetime
import json

import pandas


class QuoteEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)

        elif isinstance(o, datetime):
            return o.isoformat()

        return super(QuoteEncoder, self).default(0)


class PriceVolume(object):
    price: Decimal
    volume: Decimal

    def __init__(self, price, volume):
        self._price = price
        self._volume = volume

    @property
    def price(self):
        return self._price

    @property
    def volume(self):
        return self._volume

    def __repr__(self):
        return '{}@{}'.format(self.volume, self.price)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return (self.price == other.price) and (self.volume == other.volume)

    def __ne__(self, other):
        return not self == other


class CurrencyTrade(NamedTuple):
    direction: str
    pair: str
    quantity: Decimal
    price: Decimal
    fill_ratio: float


class CurrencyBalance(object):
    """
    Model of a currency balance.
    """

    def __init__(self, currency: str, amount: Decimal):
        self._currency = currency
        self._amount = amount

    @property
    def currency(self) -> str:
        return self._currency

    @property
    def amount(self) -> Decimal:
        return self._amount

    def __repr__(self):
        return '[{} {}]'.format(self.currency, self.amount)

    def __hash__(self):
        return hash(self.currency + str(self.amount))

    def __eq__(self, other):
        return (self.currency == other.currency) and (self.amount == other.amount)

    def __ne__(self, other):
        return not self == other


class ForexQuote(object):
    """
    Models a forex quote.
    """

    def __init__(self, timestamp: datetime = None, bid: PriceVolume = None, ask: PriceVolume = None,
                 source: str = None):
        if not timestamp:
            self._timestamp = datetime.now()

        else:
            self._timestamp = timestamp

        self._bid = bid
        self._ask = ask
        self._source = source

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    @property
    def bid(self) -> PriceVolume:
        return self._bid

    @property
    def ask(self) -> PriceVolume:
        return self._ask

    @property
    def source(self) -> str:
        return self._source

    def is_complete(self) -> bool:
        return self.bid is not None and self.ask is not None

    def to_dict(self):
        quote_data = {'timestamp': self.timestamp,
                      'bid': {'price': self.bid.price, 'amount': self.bid.volume},
                      'ask': {'price': self.ask.price, 'amount': self.ask.volume},
                      'source': self.source}
        return quote_data

    def to_json(self):
        return json.dumps(self.to_dict(), cls=QuoteEncoder)

    def __repr__(self):
        return '[{}:{}/{}]'.format(self.timestamp, self.bid, self.ask)


@total_ordering
class CurrencyPair(object):
    """
    Models a currency pair.
    """

    def __init__(self, base_currency_code: str, quote_currency_code: str):
        """
        The quotation EUR/USD 1.2500 means that one euro is exchanged for 1.2500 US dollars.
        Here, EUR is the base currency and USD is the quote currency(counter currency).
        :param base_currency_code: currency that is quoted
        :param quote_currency_code: currency that is used as the reference
        """
        self._base_currency_code = base_currency_code.upper()
        self._quote_currency_code = quote_currency_code.upper()

    def buy(self, quote: ForexQuote, volume: Decimal, illimited_volume: bool = False) -> Tuple[Dict[str,
                                                                                                    Decimal], CurrencyTrade]:
        """
        Computes the balance after the buy has taken place.
        Example, provided volume is sufficient:
            quote = EUR/USD <1.15, 1.16>, volume = +1 ---> Balances: EUR = +1, USD = -1.16

        :param quote: ForexQuote instance
        :param volume:
        :param illimited_volume: emulates infinite liquidity
        :return:
        """
        price = quote.ask.price
        if illimited_volume:
            allowed_volume = volume

        else:
            allowed_volume = min(volume, quote.ask.volume)

        fill_ratio = allowed_volume / volume
        balances = {self.base: CurrencyBalance(self.base, allowed_volume).amount,
                    self.quote: CurrencyBalance(self.quote, Decimal(allowed_volume * price * -1)).amount
                    }
        trade = CurrencyTrade('buy', repr(self), allowed_volume, price, fill_ratio)
        return balances, trade

    def sell(self, quote: ForexQuote, volume: Decimal, illimited_volume: bool = False) -> Tuple[Dict[str,
                                                                                                     Decimal], CurrencyTrade]:
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
        price = quote.bid.price
        if illimited_volume:
            allowed_volume = volume

        else:
            allowed_volume = min(volume, quote.bid.volume)

        fill_ratio = allowed_volume / volume
        balances = {self.base: CurrencyBalance(self.base, Decimal(allowed_volume * -1)).amount,
                    self.quote: CurrencyBalance(self.quote, Decimal(allowed_volume * price)).amount}

        trade = CurrencyTrade('sell', repr(self), Decimal(allowed_volume * -1), price, fill_ratio)
        return balances, trade

    def buy_currency(self, currency: str, volume: Decimal, quote: ForexQuote, illimited_volume: bool = False) -> Tuple[
        Dict[str, Decimal], CurrencyTrade]:
        """

        :param currency: currency to buy
        :param volume: amount to buy denominated in currency
        :param quote: current quote (ForexQuote instance)
        :param illimited_volume: emulates infinite liquidity
        :return: resulting balance and performed trades (balance, performed_trade)
        """
        assert currency in self.assets, 'currency {} not in pair {}'.format(currency, self)
        assert volume >= 0
        logging.debug('buying {} {} using pair {}'.format(volume, currency, self))
        if currency == self.base:
            # Direct quotation
            balances, performed_trade = self.buy(quote, volume, illimited_volume)

        else:
            # Indirect quotation
            target_volume = Decimal(volume) / quote.bid.price
            balances, performed_trade = self.sell(quote, target_volume, illimited_volume)

        return balances, performed_trade

    def sell_currency(self, currency: str, volume: Decimal, quote: ForexQuote, illimited_volume: bool = False) -> Tuple[
        Dict[str, Decimal], CurrencyTrade]:
        """

        :param currency:
        :param volume: amount to buy denominated in currency
        :param quote: current quote (ForexQuote instance)
        :param illimited_volume: emulates infinite liquidity
        :return: resulting balance and performed trades (balance, performed_trade)
        """
        assert currency in self.assets, 'currency {} not in pair {}'.format(currency, self)
        assert volume >= 0
        logging.debug('selling {} {} using pair {}'.format(volume, currency, self))
        if currency == self.base:
            # Direct quotation
            balance, performed_trade = self.sell(quote, volume, illimited_volume)

        else:
            # Indirect quotation
            target_volume = Decimal(volume) / quote.ask.price
            balance, performed_trade = self.buy(quote, target_volume, illimited_volume)

        return balance, performed_trade

    def convert(self, currency: str, amount: Decimal, quote: ForexQuote):
        if currency == self.base:
            destination_currency = self.quote

        else:
            destination_currency = self.base

        if amount >= 0:
            balances, trade = self.sell_currency(currency, amount, quote, illimited_volume=True)
            amount = balances[destination_currency]
            return abs(amount)

        else:
            balances, trade = self.buy_currency(currency, abs(amount), quote, illimited_volume=True)
            amount = balances[destination_currency]
            return abs(amount) * -1

    @property
    def assets(self) -> Set[str]:
        return {self._base_currency_code, self._quote_currency_code}

    @property
    def quote(self) -> str:
        return self._quote_currency_code

    @property
    def base(self) -> str:
        return self._base_currency_code

    def to_direct(self, separator: str = '/') -> str:
        return '{}{}{}'.format(self.base, separator, self.quote)

    def to_indirect(self, separator: str = '/') -> str:
        return '{}{}{}'.format(self.quote, separator, self.base)

    def __repr__(self):
        return '<{}/{}>'.format(self.base, self.quote)

    def __hash__(self):
        return hash(self.base + ',' + self.quote)

    def __eq__(self, other):
        return (self.base == other.base) and (self.quote == other.quote)

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return repr(self) <= repr(other)


@total_ordering
class ArbitrageStrategy(object):
    """
    Models an arbitrage strategy.
    """

    def __init__(self, pair1: CurrencyPair, pair2: CurrencyPair, pair3: CurrencyPair):
        """

        :param pair1: CurrencyPair instance
        :param pair2: CurrencyPair instance
        :param pair3: CurrencyPair instance
        """
        pairs = {pair1, pair2, pair3}
        bases = [pair.base for pair in pairs]
        bases_count = dict((base, len(list(group))) for base, group in itertools.groupby(sorted(bases)))
        unique_bases = sorted([base for base in bases_count if bases_count[base] == 1])
        if len(unique_bases) == 0:
            raise SystemError('cannot arbitrage pairs: {}'.format(pairs))

        common_currency = unique_bases[0]
        direct_pair = None
        for pair in pairs:
            if common_currency not in pair.assets:
                direct_pair = pair
                break

        if direct_pair is None:
            raise SystemError('cannot arbitrage pairs: {}'.format(pairs))

        self._pair1 = direct_pair
        indirect_pairs = list(pairs.difference({direct_pair}))
        if common_currency == indirect_pairs[0].quote:
            self._pair2, self._pair3 = indirect_pairs[0], indirect_pairs[1]

        else:
            self._pair3, self._pair2 = indirect_pairs[0], indirect_pairs[1]

        self._quotes = {
            self._pair1: ForexQuote(),
            self._pair2: ForexQuote(),
            self._pair3: ForexQuote()
        }

    def __repr__(self):
        return '[{},{}]'.format(self.indirect_pairs, self.direct_pair)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return self.indirect_pairs == other.indirect_pairs and self.direct_pair == other.direct_pair

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return repr(self) <= repr(other)

    @property
    def direct_pair(self) -> CurrencyPair:
        return self._pair1

    @property
    def indirect_pairs(self) -> Tuple[CurrencyPair, CurrencyPair]:
        return self._pair2, self._pair3

    @property
    def pairs(self) -> Tuple[CurrencyPair, CurrencyPair, CurrencyPair]:
        sorted_pairs = sorted([self._pair1, self._pair2, self._pair3])
        return sorted_pairs[0], sorted_pairs[1], sorted_pairs[2]

    def update_quote(self, pair: CurrencyPair, quote: ForexQuote) -> None:
        """

        :param pair:
        :param quote:
        :return:
        """
        self._quotes[pair] = quote

    @property
    def quotes(self) -> Dict[CurrencyPair, ForexQuote]:
        """

        :return:
        """
        return self._quotes

    @property
    def quotes_valid(self) -> bool:
        """

        :return:
        """
        is_valid = True
        for pair, quote in self.quotes.items():
            is_valid = is_valid and quote.is_complete()

        return is_valid

    def find_opportunity(self, initial_amount: Decimal, illimited_volume: bool, skip_capped: bool = True) -> Tuple[Any, Any]:
        """

        :param initial_amount: amount to be invested
        :param illimited_volume: emulates infinite liquidity
        :param skip_capped:
        :return:
        """
        opportunity = None, None
        if self.quotes_valid:
            balances_df, trades_df = self.apply_arbitrage(initial_amount, illimited_volume=illimited_volume)
            balances_by_currency = balances_df.sum(axis=1)
            if not skip_capped or trades_df[trades_df['fill_ratio'] < 1].count() == 0:
                logging.info('adding new opportunity:\n{}'.format(trades_df))
                logging.info('resulting balances:\n{}'.format(balances_by_currency))
                opportunity = trades_df.to_dict(orient='records'), balances_by_currency.to_dict()

            else:
                logging.info('no opportunity')

        else:
            logging.info('incomplete quotes')

        return opportunity

    def apply_arbitrage(self, initial_amount: Decimal, illimited_volume: bool) -> Tuple[Any, Any]:
        """
        Determines arbitrage operations:
            - selling indirect pair 1
            - selling indirect pair 2
            - offsetting remaining balance
        :param initial_amount:
        :param illimited_volume:
        :return:
        """
        logging.info('accumulating currency: {}'.format(self.direct_pair.quote))
        balance_initial, trade_initial = self.indirect_pairs[0].sell(self.quotes[self.indirect_pairs[0]],
                                                                     initial_amount, illimited_volume)
        logging.info('balance step 1: {}'.format(balance_initial))
        balance_next, trade_next = self.indirect_pairs[1].sell(self.quotes[self.indirect_pairs[1]],
                                                               balance_initial[self.indirect_pairs[0].quote],
                                                               illimited_volume)
        logging.info('balance step 2: {}'.format(balance_next))
        if self.direct_pair.base in balance_initial:
            settling_amount = balance_initial[self.direct_pair.base]
            balance_final, trade_final = self.direct_pair.buy_currency(self.direct_pair.base, abs(settling_amount),
                                                                       self.quotes[self.direct_pair], illimited_volume)

        else:
            settling_amount = balance_initial[self.direct_pair.quote]
            balance_final, trade_final = self.direct_pair.buy_currency(self.direct_pair.quote, abs(settling_amount),
                                                                       self.quotes[self.direct_pair], illimited_volume)

        logging.info('balance step 3: {}'.format(balance_final))
        balance1_series = pandas.Series(balance_initial, name='initial')
        balance2_series = pandas.Series(balance_next, name='next')
        balance3_series = pandas.Series(balance_final, name='final')
        balance_series = [balance1_series, balance2_series, balance3_series]
        balances_df = pandas.concat(balance_series, axis=1)
        trades_df = pandas.DataFrame([trade_initial, trade_next, trade_final])
        return balances_df, trades_df


class CurrencyConverter(object):
    """
    Forex conversion.
    """

    def __init__(self, market: Tuple[str, str], order_book_callback: Callable[[CurrencyPair], ForexQuote],
                 direct: bool = True):
        """

        :param market_name: market name is the currency pair, for example ('USD', 'EUR')
        :param order_book_callback: function returning a quote for a given CurrencyPair
        :param direct: when foreign currency comes first in market name
        """
        if direct:
            self._domestic_currency, self._foreign_currency = market[0].upper(), market[1].upper()

        else:
            self._foreign_currency, self._domestic_currency = market[0].upper(), market[1].upper()

        self._order_book_callback = order_book_callback

    @property
    def domestic_currency(self) -> str:
        return self._domestic_currency

    @property
    def foreign_currency(self) -> str:
        return self._foreign_currency

    def exchange(self, currency: str, amount: Decimal) -> Decimal:
        """

        :param currency:
        :param amount:
        :return:
        """
        if currency == self.domestic_currency:
            return amount

        elif currency == self.foreign_currency:
            target_pair = CurrencyPair(self.domestic_currency, currency)

        else:
            raise LookupError('unable to convert {}'.format(currency))

        quote = self._order_book_callback(target_pair)
        return target_pair.convert(currency, amount, quote)

    def sell(self, currency: str, amount: Decimal) -> Decimal:
        assert amount >= 0
        return self.exchange(currency.upper(), amount)

    def buy(self, currency: str, amount: Decimal) -> Decimal:
        assert amount >= 0
        return self.exchange(currency.upper(), -amount)


def order_entries(quotes: Dict[Decimal, Tuple[datetime, Decimal, int]], reverse=False) -> List[Dict[str, Any]]:
    ordered_quotes = list()
    for price in sorted(quotes, reverse=reverse):
        ordered_quotes.append(quotes[price])

    return ordered_quotes


class OrderBook(object):
    """
    Models an order book.
    """

    def __init__(self, pair: CurrencyPair, source: str):
        self._quotes_bid_by_price = dict()
        self._quotes_ask_by_price = dict()
        self._pair = pair
        self._source = source

    @property
    def source(self) -> str:
        return self._source

    @property
    def pair(self) -> CurrencyPair:
        return self._pair

    @property
    def quotes_bid(self) -> List[Dict[str, Any]]:
        quotes_bid = order_entries(self._quotes_bid_by_price, reverse=True)
        return quotes_bid

    @property
    def quotes_ask(self) -> List[Dict[str, Any]]:
        quotes_ask = order_entries(self._quotes_ask_by_price, reverse=False)
        return quotes_ask

    def load_snapshot(self, snapshot) -> None:
        """

        :param snapshot:
        :return:
        """
        channel_id, book_data = snapshot
        for price, count, amount in book_data:
            timestamp = datetime.utcnow()
            if Decimal(amount) > 0:
                self._quotes_bid_by_price[Decimal(price)] = {'timestamp': timestamp, 'price': Decimal(price),
                                                             'amount': Decimal(amount)}

            else:
                self._quotes_ask_by_price[Decimal(price)] = {'timestamp': timestamp, 'price': Decimal(price) * -1,
                                                             'amount': Decimal(amount)}

    def remove_quote(self, price: Decimal, quotes_by_price: Dict[Decimal, ForexQuote]) -> bool:
        """

        :param price:
        :param quotes_by_price:
        :return:
        """
        if price in quotes_by_price:
            quotes_by_price.pop(price)
            return True

        return False

    def remove_bid(self, price: Decimal) -> bool:
        """

        :param price:
        :return:
        """
        return self.remove_quote(price, self._quotes_bid_by_price)

    def remove_ask(self, price: Decimal) -> bool:
        """

        :param price:
        :return:
        """
        return self.remove_quote(price, self._quotes_ask_by_price)

    def update_quote(self, quotes_by_price: Dict[Decimal, Dict[str, Any]], price: Decimal, amount: Decimal) -> bool:
        """

        :param quotes_by_price:
        :param price:
        :param amount:
        :return:
        """
        timestamp = datetime.utcnow()
        if price in self._quotes_bid_by_price:
            quotes_by_price[price]['timestamp'] = timestamp
            quotes_by_price[price]['amount'] = amount

        else:
            quotes_by_price[price] = {'timestamp': timestamp, 'price': price, 'amount': amount}

        return True

    def update_bid(self, price: Decimal, amount: Decimal) -> bool:
        """

        :param price:
        :param amount:
        :return:
        """
        return self.update_quote(self._quotes_bid_by_price, price, amount)

    def update_ask(self, price: Decimal, amount: Decimal) -> bool:
        """

        :param price:
        :param amount:
        :return:
        """
        return self.update_quote(self._quotes_bid_by_price, price, amount)

    def level_one(self) -> ForexQuote:
        """
        :return:
        """
        best_bid = self.quotes_bid[0]
        best_ask = self.quotes_ask[0]
        timestamp = max(best_bid['timestamp'], best_ask['timestamp'])
        bid_side = PriceVolume(abs(best_bid['price']), abs(best_bid['amount']))
        ask_side = PriceVolume(abs(best_ask['price']), abs(best_ask['amount']))
        return ForexQuote(timestamp, bid_side, ask_side, source=self.source)

    def to_json(self) -> str:
        return json.dumps({'bid': self.quotes_bid, 'ask': self.quotes_ask}, cls=QuoteEncoder)

    def __repr__(self) -> str():
        return self.to_json()
