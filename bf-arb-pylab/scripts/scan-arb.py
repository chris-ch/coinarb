import argparse
import logging

import sys
import requests_cache

from arbitrage import parse_strategy, parse_quote


def main(args):
    if args.strategy:
        strategy = parse_strategy(args.strategy.upper())

        if args.replay:
            prices_input = open(args.replay, 'r')

        else:
            logging.info('loading prices from standard input')
            prices_input = sys.stdin

        for line in prices_input:
            logging.info('receiving update')
            logging.info('strategy book: {}'.format(strategy.quotes))
            pair, quote = parse_quote(line)
            strategy.update_quote(pair, quote)
            strategy.find_opportunity(illimited_volume=False, skip_capped=False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('scan-arb.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    parser = argparse.ArgumentParser(description='Scanning arbitrage opportunities.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')
    parser.add_argument('--strategy', type=str, help='strategy as a formatted string (for example: eth/btc,btc/usd,eth/usd')
    parser.add_argument('--replay', type=str, help='use recorded prices')

    args = parser.parse_args()
    # DEBUGGING
    requests_cache.install_cache('demo_cache')

    main(args)
