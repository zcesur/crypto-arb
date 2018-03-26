import time
from multiprocessing import Pool
from collections import namedtuple
from operator import attrgetter

from exchange import Bittrex, Kraken
from util import initialize_logger

ArbOpp = namedtuple('ArbOpp', [
    'pnl', 'size', 'currency', 'orig_rate', 'dest_rate', 'destination',
    'spread_pct'
])
exchanges = ['Bittrex', 'Kraken']
x_map = {'Bittrex': Bittrex(), 'Kraken': Kraken()}
currencies = ['XRP', 'XLM']
minimum_order_size = {'XRP': 30, 'XLM': 300}
fees = {
    'Bittrex': {
        'XLM': 0.01,
        'XRP': 1
    },
    'Kraken': {
        'XLM': 0.01,
        'XRP': 0.02
    },
}

logger = initialize_logger('MAIN')


def arbitrage(origin):
    while True:
        start = time.time()
        assert origin in exchanges, 'Invalid origin.'

        # List of exchanges that we could potentially sell on
        destinations = exchanges[:]
        destinations.remove(origin)
        destination_apis = map(lambda x: x_map[x], destinations)
        origin_api = x_map[origin]

        opps = arb_opportunities(
            currencies=currencies,
            orig_api=origin_api,
            dest_apis=destination_apis)
        best_opp = opps[0]
        end = time.time()

        if best_opp.pnl > 0 and best_opp.spread_pct >= 0.01:
            print('Origin: {} - Start time: {} - Time elapsed: {}\n'
                  'Best opportunity: {}\n').format(origin, str(start),
                                                   str(end - start),
                                                   str(best_opp))

            logger.info(
                ('Initiating the trade for the best opportunity. '
                 'currency:{}, origin:{}, destination:{}, spread_pct:{:.4f} '
                 'estimated_pnl:{:.8f}, size:{:2f}').format(
                     best_opp.currency, origin, best_opp.destination,
                     best_opp.spread_pct, best_opp.pnl, best_opp.size))
            x_map[origin].buy(
                currency=best_opp.currency,
                size=best_opp.size,
                rate=best_opp.orig_rate)
            x_map[best_opp.destination].sell(
                currency=best_opp.currency,
                size=best_opp.size,
                rate=best_opp.dest_rate)

            time.sleep(10)
            balance = x_map[origin].balances(
                currencies=[best_opp.currency])[best_opp.currency]
            x_map[origin].withdraw(
                currency=best_opp.currency,
                size=min(best_opp.size, balance),
                destination=best_opp.destination)

        else:
            logger.debug(
                'There exist no profitable spreads from {} at the moment.'.
                format(origin))

        time.sleep(30)


def arb_opportunities(currencies, orig_api, dest_apis):
    orig_bal = orig_api.balances(currencies=['BTC'])['BTC']
    dest_bals = {
        ex.name: ex.balances(currencies=currencies)
        for ex in dest_apis
    }

    orig_rates = orig_api.asks(currencies=currencies)
    dest_rates = {ex.name: ex.bids(currencies=currencies) for ex in dest_apis}

    data = []
    for exchange, rates in dest_rates.items():
        for currency, rate in rates.items():
            data.append({
                'origin': orig_api.name,
                'destination': exchange,
                'currency': currency,
                'orig_rate': orig_rates[currency],
                'dest_rate': rate,
                'orig_bal': orig_bal,
                'dest_bal': dest_bals[exchange][currency],
            })

    pool = Pool()
    result = pool.map(calc_pnl_unpack, data)
    pool.close()
    pool.join()
    return sorted(result, key=attrgetter('pnl'), reverse=True)


def calc_pnl_unpack(kwargs):
    def calc_pnl(origin, destination, currency, orig_rate, dest_rate, orig_bal,
                 dest_bal):
        # orig_size = min(orig_bal, 0.1) / orig_rate
        orig_size = orig_bal / orig_rate
        dest_size = dest_bal
        size = int(min(orig_size, dest_size))

        if size < minimum_order_size[currency]:
            size = 0

        commission = 0.0020
        spread = dest_rate - orig_rate
        spread_pct = dest_rate / orig_rate - 1
        pnl = spread * size \
                - fees[origin][currency] * orig_rate \
                - (dest_rate + orig_rate) * size * commission

        if pnl > 0 and spread_pct >= 0.01:
            logger.debug(
                ('Found a profitable spread. '
                 'currency:{}, origin:{}, destination:{}, spread_pct:{:.4f} '
                 'estimated_pnl:{:.8f}').format(currency, origin, destination,
                                                spread_pct, pnl))

        return ArbOpp(
            pnl=pnl,
            size=size,
            currency=currency,
            orig_rate=orig_rate,
            dest_rate=dest_rate,
            destination=destination,
            spread_pct=spread_pct)

    return calc_pnl(**kwargs)
