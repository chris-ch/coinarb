import itertools
import logging
import re

from arbitrage.entities import ArbitrageStrategy, CurrencyPair


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


def parse_strategy(strategy_string: str):
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
            logging.debug('incompatible combination: {}, {}, {} not in {}'.format(common_pair, indirect_pair_1, indirect_pair_2, pairs))
            continue


def scan_arbitrage_opportunities(strategies, order_book_callbak, illimited_volume):
    """
    Scanning arbitrage opportunities over the indicated pairs.

    :param strategies: iterable of pairs triplet
    :param order_book_callbak:
    :param illimited_volume: emulates infinite liquidity
    :return:
    """
    results = list()
    for strategy in strategies:
        strategy.update_quotes(order_book_callbak)
        if strategy.quotes_valid:
            opportunities = strategy.find_opportunities(illimited_volume)
            if opportunities is not None and len(opportunities) > 0:
                results += opportunities

    return results
