import logging

import itertools

import numpy
import pandas


def trade_pair(pair_code, bid, ask, volume):
    """
    Computes the balance after the operation takes place.
    Example:
        XXLMXXBT 38092.21 0.000008210 0.000008340 121.618 --> With a volume of 1 we go long 0.000008210 XXBT and short 1 XXLM

    :param pair_code:
    :param bid:
    :param ask:
    :param volume:
    :return:
    """
    currency_first = pair_code[:4]
    currency_second = pair_code[4:]
    balance = {currency_first: 0, currency_second: 0}
    trade = None
    if volume > 0:
        allowed_volume = min(volume, bid['volume'])
        capped = numpy.NaN
        if allowed_volume < volume:
            capped = allowed_volume

        balance = {currency_first: allowed_volume * -1, currency_second: allowed_volume * bid['price']}
        trade = {'direction': 'buy', 'pair': pair_code, 'quantity': allowed_volume, 'price': bid['price'],
                 'capped': capped}

    elif volume < 0:
        allowed_volume = min(abs(volume), ask['volume'])
        capped = numpy.NaN
        if allowed_volume < abs(volume):
            capped = allowed_volume

        balance = {currency_first: allowed_volume, currency_second: allowed_volume * ask['price'] * -1}
        trade = {'direction': 'sell', 'pair': pair_code, 'quantity': allowed_volume, 'price': ask['price'],
                 'capped': capped}

    return balance, trade


def buy_currency_using_pair(currency, volume, pair_code, bid, ask):
    """

    :param currency:
    :param volume: amount to buy denominated in currency
    :param pair_code:
    :param bid:
    :param ask:
    :return:
    """
    logging.info('buying {} {} using {}'.format(volume, currency, pair_code))
    if pair_code[4:] == currency:
        # Direct quotation
        logging.debug('direct quotation')
        target_volume = volume / bid['price']
        balance, performed_trade = trade_pair(pair_code, bid, ask, round(target_volume, 10))

    else:
        # Indirect quotation
        logging.debug('indirect quotation')
        balance, performed_trade = trade_pair(pair_code, bid, ask, volume * -1)

    return balance, performed_trade


def sell_currency_using_pair(currency, volume, pair_code, bid, ask):
    """

    :param currency:
    :param volume: amount to buy denominated in currency
    :param pair_code:
    :param bid:
    :param ask:
    :return:
    """
    logging.info('selling {} {} using {}'.format(volume, currency, pair_code))
    if pair_code[4:] == currency:
        # Direct quotation
        logging.debug('direct quotation')
        target_volume = -1 * volume / ask['price']
        balance, performed_trade = trade_pair(pair_code, bid, ask, round(target_volume, 10))

    else:
        # Indirect quotation
        logging.debug('indirect quotation')
        balance, performed_trade = trade_pair(pair_code, bid, ask, volume)

    return balance, performed_trade


def calculate_arbitrage_opportunity(pair_1, pair_bid_1, pair_ask_1, pair_2, pair_bid_2, pair_ask_2, pair_3, pair_bid_3,
                                    pair_ask_3, skip_capped=True):
    """

    :param pair_1:
    :param pair_bid_1:
    :param pair_ask_1:
    :param pair_2:
    :param pair_bid_2:
    :param pair_ask_2:
    :param pair_3:
    :param pair_bid_3:
    :param pair_ask_3:
    :param skip_capped:
    :return: (trades, balances)
    """
    pairs = [pair_1, pair_2, pair_3]
    pair_bids = [pair_bid_1, pair_bid_2, pair_bid_3]
    pair_asks = [pair_ask_1, pair_ask_2, pair_ask_3]
    results = list()
    for first, second, third in itertools.permutations([0, 1, 2]):
        currency_initial = pairs[first][4:]
        initial_bid = pair_bids[first]
        initial_ask = pair_asks[first]
        if currency_initial in pairs[second]:
            next_pair = pairs[second]
            next_bid = pair_bids[second]
            next_ask = pair_asks[second]
            final_pair = pairs[third]
            final_bid = pair_bids[third]
            final_ask = pair_asks[third]

        else:
            next_pair = pairs[third]
            next_bid = pair_bids[third]
            next_ask = pair_asks[third]
            final_pair = pairs[second]
            final_bid = pair_bids[second]
            final_ask = pair_asks[second]

        if next_pair[:len(next_pair) // 2] != currency_initial:
            currency_next = next_pair[:len(next_pair) // 2]

        else:
            currency_next = next_pair[len(next_pair) // 2:]

        balance_initial, trade_initial = buy_currency_using_pair(currency_initial, 1, pairs[first], initial_bid,
                                                                 initial_ask)
        balance_next, trade_next = sell_currency_using_pair(currency_initial, balance_initial[currency_initial],
                                                            next_pair, next_bid, next_ask)
        balance_final, trade_final = sell_currency_using_pair(currency_next, balance_next[currency_next], final_pair,
                                                              final_bid, final_ask)

        balance1_series = pandas.Series(balance_initial, name='initial')
        balance2_series = pandas.Series(balance_next, name='next')
        balance3_series = pandas.Series(balance_final, name='final')
        balances = pandas.concat([balance1_series, balance2_series, balance3_series], axis=1)
        trades_df = pandas.DataFrame([trade_initial, trade_next, trade_final])
        if not skip_capped or trades_df['capped'].count() == 0:
            results.append((trades_df, balances.sum(axis=1)))

    return results


def scan_arbitrage_opportunities(pairs, order_book_callbak):
    """
    Scanning arbitrage opportunities over the indicated pairs.

    :param pairs: set of (base code, quote code)
    :param order_book_callbak:
    :return:
    """
    assets = set()
    for base_code, quote_code in pairs:
        assets.add(base_code)
        assets.add(quote_code)

    pair_codes = {(base_code + quote_code) for base_code, quote_code in pairs}
    logging.info('available pairs ({}): {}'.format(len(pairs), pairs))
    logging.info('available assets ({}): {}'.format(len(assets), assets))
    results = list()
    for common_leg in assets:
        for leg_pair1 in assets:
            if leg_pair1 == common_leg:
                continue

            for leg_pair2 in assets:
                if leg_pair2 == common_leg or leg_pair2 == leg_pair1:
                    continue

                logging.debug('checking legs: {}, {}, {}'.format(leg_pair1, common_leg, leg_pair2))

                direct_pair = leg_pair1 + leg_pair2
                indirect_pair_1 = leg_pair1 + common_leg
                indirect_pair_2 = leg_pair2 + common_leg
                if pair_codes.issuperset({direct_pair, indirect_pair_1, indirect_pair_2}):
                    logging.info('trying pair {} with {} and {}'.format(direct_pair, indirect_pair_1, indirect_pair_2))
                    direct_bid, direct_ask = order_book_callbak(direct_pair)
                    if direct_bid is None or direct_ask is None:
                        continue

                    indirect_bid_1, indirect_ask_1 = order_book_callbak(indirect_pair_1)
                    if indirect_bid_1 is None or indirect_ask_1 is None:
                        continue

                    indirect_bid_2, indirect_ask_2 = order_book_callbak(indirect_pair_2)
                    if indirect_bid_2 is None or indirect_ask_2 is None:
                        continue

                    arbitrage_ratio = calculate_arbitrage_opportunity(direct_pair, direct_bid, direct_ask,
                                                                      indirect_pair_1, indirect_bid_1, indirect_ask_1,
                                                                      indirect_pair_2, indirect_bid_2, indirect_ask_2)

                    results.append(arbitrage_ratio)

                else:
                    logging.debug('no combination found for {}, {}, {}'.format(pairs, direct_pair, indirect_pair_1, indirect_pair_2))

    return results
