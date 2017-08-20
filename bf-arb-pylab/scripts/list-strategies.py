import argparse
import logging

import bitfinex
import tenacity

from arbitrage import parse_pair_from_indirect, create_strategies


def parse_symbols_bitfinex(pairs):
    symbols = set()
    for pair_code in pairs:
        symbols.add(parse_pair_from_indirect(pair_code))

    return symbols


@tenacity.retry(wait=tenacity.wait_fixed(1), stop=tenacity.stop_after_attempt(5))
def main(args):
    bitfinex_client = bitfinex.Client()
    pair_codes = bitfinex_client.symbols()
    pairs = parse_symbols_bitfinex(pair_codes)
    strategies = create_strategies(pairs)
    for strategy in strategies:
        print('[{}]'.format(','.join(['"{}/{}"'.format(pair.base, pair.quote) for pair in strategy.pairs])))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', filename='list-strategies.log')
    logging.getLogger('requests').setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description='Listing available strategies.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    args = parser.parse_args()
    main(args)
