import itertools
import json
import logging
import re
from decimal import Decimal
import dateutil.parser
from typing import Generator, Iterable, Tuple

from arbitrage.entities import ArbitrageStrategy, CurrencyPair, CurrencyConverter, ForexQuote, OrderBook, PriceVolume


def parse_pair_from_indirect(pair_code: str) -> CurrencyPair:
    """

    :param pair_code: concatenated currency codes, base last
    :return:
    """
    return CurrencyPair(pair_code[len(pair_code) // 2:], pair_code[:len(pair_code) // 2])


def parse_pair_from_direct(pair_code: str) -> CurrencyPair:
    """

    :param pair_code: concatenated currency codes, base first
    :return:
    """
    return CurrencyPair(pair_code[:len(pair_code) // 2], pair_code[len(pair_code) // 2:])


def parse_currency_pair(pair_string: str, separator: str='/', indirect_mode: bool=False) -> CurrencyPair:
    """

    :param pair_string: format <pair_1/pair_2> with / used as separator
    :param separator:
    :param indirect_mode: quote currency comes first
    :return:
    """
    pattern = re.match('^<?([a-zA-Z0-9]+){}([a-zA-Z0-9]+)>?$'.format(separator), pair_string.strip())
    pair1, pair2 = pattern.group(1), pattern.group(2)
    if indirect_mode:
        pair1, pair2 = pair2, pair1

    return CurrencyPair(pair1, pair2)


def parse_strategy(strategy_string: str, indirect_mode: bool=False) -> ArbitrageStrategy:
    """
    :param strategy_string: example [<btc/eth>,<usd/btc>,<usd/eth>]
    :param indirect_mode: quote currency comes first
    :return:
    """
    pattern = re.match(r'^\[?(.*),(.*),([^]]+)\]?$', strategy_string.strip())
    pair1_code = pattern.group(1)
    pair2_code = pattern.group(2)
    pair3_code = pattern.group(3)
    pair1 = parse_currency_pair(pair1_code.strip(), indirect_mode=indirect_mode)
    pair2 = parse_currency_pair(pair2_code.strip(), indirect_mode=indirect_mode)
    pair3 = parse_currency_pair(pair3_code.strip(), indirect_mode=indirect_mode)
    return ArbitrageStrategy(pair1, pair2, pair3)


def create_strategies(pairs: Iterable[CurrencyPair]) -> Generator[ArbitrageStrategy, None, None]:
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


def parse_quote(line: str) -> Tuple[CurrencyPair, ForexQuote]:
    """

    :param line:
    :return:
    """
    data = json.loads(line.strip())
    timestamp = dateutil.parser.parse(data['timestamp'])
    bid = PriceVolume(Decimal(data['bid']['price']), Decimal(data['bid']['amount']))
    ask = PriceVolume(Decimal(data['ask']['price']), Decimal(data['ask']['amount']))
    quote = ForexQuote(timestamp=timestamp, bid=bid, ask=ask, source=data['source'])
    pair = parse_currency_pair(data['pair'], separator='/')
    return pair, quote