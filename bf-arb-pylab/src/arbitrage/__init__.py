import itertools
import logging
import re
from time import sleep
from typing import Callable, Any

from arbitrage.entities import ArbitrageStrategy, CurrencyPair, CurrencyConverter, ForexQuote


def parse_pair_from_indirect(pair_code):
    """

    :param pair_code: concatenated currency codes, base last
    :return:
    """
    return CurrencyPair(pair_code[len(pair_code) // 2:], pair_code[:len(pair_code) // 2])


def parse_pair_from_direct(pair_code):
    """

    :param pair_code: concatenated currency codes, base first
    :return:
    """
    return CurrencyPair(pair_code[:len(pair_code) // 2], pair_code[len(pair_code) // 2:])


def parse_currency_pair(pair_string, separator='/'):
    """

    :param pair_string: format <pair_1/pair_2> with / used as separator
    :param separator:
    :return:
    """
    pair1, pair2 = pair_string.split(separator)
    return CurrencyPair(pair1[1:], pair2[:-1])


def parse_strategy(strategy_string):
    """

    :param strategy_string: example [<btc/eth>,<usd/btc>,<usd/eth>]
    :return:
    """
    pattern = re.match(r'^\[(.*),(.*),(.*)\]$', strategy_string.strip())
    pair1 = pattern.group(1)
    pair2 = pattern.group(2)
    pair3 = pattern.group(3)
    return ArbitrageStrategy(parse_currency_pair(pair1), parse_currency_pair(pair2), parse_currency_pair(pair3))


def create_strategies(pairs):
    """

    :param assets: iterable currency codes
    :param pairs: set of CurrencyPair instances
    :return: strategy (common_pair, indirect_pair_1, indirect_pair_2)
    """
    pairs = set(pairs)
    assets = set()
    for pair in pairs:
        assets.update(pair.assets)

    logging.info('available pairs ({}): {}'.format(len(pairs), pairs))
    logging.info('available assets ({}): {}'.format(len(assets), assets))
    for common_leg, leg_pair1, leg_pair2 in itertools.permutations(assets, 3):
        logging.debug('checking legs: {}, {}, {}'.format(common_leg, leg_pair1, leg_pair2))
        common_pair = CurrencyPair(leg_pair1, leg_pair2)
        indirect_pair_1 = CurrencyPair(leg_pair1, common_leg)
        indirect_pair_2 = CurrencyPair(leg_pair2, common_leg)

        if pairs.issuperset({common_pair, indirect_pair_1, indirect_pair_2}):
            yield ArbitrageStrategy(common_pair, indirect_pair_1, indirect_pair_2)

        else:
            logging.debug(
                'incompatible combination: {}, {}, {} not in {}'.format(common_pair, indirect_pair_1, indirect_pair_2,
                                                                        pairs))
            continue


def scan_arbitrage_opportunities(strategy, order_book_callbak: Callable[[Any], Callable[[CurrencyPair], ForexQuote]],
                                 bitfinex_client, illimited_volume):
    """
    Scanning arbitrage opportunities over the indicated pairs.

    :param strategy: iterable of pairs triplet
    :param order_book_callbak:
    :param illimited_volume: emulates infinite liquidity
    :return:
    """
    while True:  # TODO use websocket API instead
        quotes = order_book_callbak(bitfinex_client)
        strategy.update_quotes(quotes)
        if strategy.quotes_valid:
            result = strategy.find_opportunities(illimited_volume)
            for trades, balances in result:
                market = ('btc', balances['currency'])
                converter = CurrencyConverter(market, order_book_callbak(bitfinex_client))
                bitcoin_amount = converter.exchange(balances['currency'], balances['remainder'])
                if bitcoin_amount > 0:
                    logging.info('residual value: {}'.format(bitcoin_amount))
                    logging.info('trades:\n{}'.format(trades))
        logging.info('---------------- sleeping 1 second ----------------')
        sleep(1)
