import argparse
import logging

import sys
from collections import defaultdict
from decimal import Decimal
from datetime import datetime

from arbitrage import parse_strategy, parse_quote_json


def main(args):
    thresholds = defaultdict(float)
    if args.threshold:
        for threshold in args.threshold:
            currency, amount = threshold.split(':')
            thresholds[currency.upper()] = Decimal(amount)

    logging.info('applying thresholds: {}'.format(thresholds))
    if args.strategy:
        strategy = parse_strategy(args.strategy.upper())
        logging.info('starting strategy: {}'.format(strategy))
        if args.replay:
            prices_input = open(args.replay, 'r')

        else:
            logging.info('loading prices from standard input')
            prices_input = sys.stdin

        for line in prices_input:
            if len(line.strip()) == 0:
                continue

            logging.debug('received update: {}'.format(line))
            pair, quote = parse_quote_json(line)
            strategy.update_quote(pair, quote)
            target_trades, target_balances = strategy.find_opportunity(illimited_volume=False)
            if target_balances is None:
                continue

            enable_trades = False
            for currency in target_balances:
                if target_balances[currency] > thresholds['currency']:
                    enable_trades = True
                    break

            if enable_trades:
                now = datetime.now()
                print('{}: {}'.format(now, target_trades))

    else:
        logging.info('no strategy provided: terminating')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', filename='scan-arb.log', filemode='w')
    logging.getLogger('requests').setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    parser = argparse.ArgumentParser(description='Scanning arbitrage opportunities.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--config', type=str, help='configuration file', default='config.json')
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secrets.json')
    parser.add_argument('--strategy', type=str, help='strategy as a formatted string (for example: eth/btc,btc/usd,eth/usd')
    parser.add_argument('--replay', type=str, help='use recorded prices')
    parser.add_argument('--threshold', action='append', help='lower profit limit for given currency (ex: "USD:0.02")')
    parser.add_argument('--amount', type=str, help='maximum amount for trading expressed in indirect pair 1 quoted currency', default=Decimal(1))

    args = parser.parse_args()
    main(args)
