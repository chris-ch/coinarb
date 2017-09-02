import argparse
import logging


def main(args):
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', filename='poll-trades.log', filemode='w')
    logging.getLogger('requests').setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    parser = argparse.ArgumentParser(description='Executing trades.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    parser.add_argument('--secrets', type=str, help='configuration with secret connection data', default='secret.json')

    args = parser.parse_args()
    main(args)
