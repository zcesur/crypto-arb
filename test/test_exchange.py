import unittest
import json
import logging

from cryptoarb.exchange import Bittrex, Kraken


class BittrexTestAPI(Bittrex):
    def __init__(self):
        self.client = BittrexTestClient()
        self.logger = logging.getLogger('test')


class KrakenTestAPI(Kraken):
    def __init__(self):
        self.client = KrakenTestClient()
        self.logger = logging.getLogger('test')


class BaseTestClient(object):
    def __init__(self, name):
        self.name = name

    def fetch_sample_response(self, route):
        with open("test/sample_responses/%s/%s.json" % (self.name,
                                                        route)) as f:
            return json.load(f)


class BittrexTestClient(BaseTestClient):
    def __init__(self):
        self.name = 'bittrex'

    def get_balances(self):
        return self.fetch_sample_response('getbalances')

    def buy_limit(self, market, quantity, rate):
        return self.fetch_sample_response('buylimit')

    def sell_limit(self, market, quantity, rate):
        return self.fetch_sample_response('selllimit')

    def cancel(self, uuid):
        return self.fetch_sample_response('cancel')

    def withdraw(self, currency, quantity, address, paymentid):
        return self.fetch_sample_response('withdraw')

    def get_order(self, uuid):
        return self.fetch_sample_response('getorder')


class KrakenTestClient(BaseTestClient):
    def __init__(self):
        self.name = 'kraken'

    def query_public(self, method, req=None):
        return self.fetch_sample_response(method)

    def query_private(self, method, req=None):
        return self.fetch_sample_response(method)


class BittrexTests(unittest.TestCase):
    def setUp(self):
        self.api = BittrexTestAPI()

    def test_balances(self):
        balances = self.api.balances(currencies=['BTC', 'XRP'])
        self.assertTrue(set(balances.keys()) == set(['BTC', 'XRP']))
        self.assertTrue(all(isinstance(v, float) for v in balances.values()))

    def test_buy(self):
        uuid = self.api.buy(currency="XLM", size=0, rate=0.5)
        self.assertTrue(isinstance(uuid, basestring))

    def test_sell(self):
        uuid = self.api.sell(currency="XLM", size=0, rate=0.5)
        self.assertTrue(isinstance(uuid, basestring))

    def test_cancel(self):
        status = self.api.cancel(uuid="abc")
        self.assertTrue(status)

    def test_withdraw(self):
        uuid = self.api.withdraw(currency="XLM", size=0, destination="Kraken")
        self.assertTrue(isinstance(uuid, basestring))

    def test_get_order(self):
        data = self.api.get_order(uuid="abc")
        required_keys = ['open_size', 'fill_size', 'price_per_unit']
        self.assertTrue(all(k in data for k in required_keys))
        if data['fill_size'] == 0:
            self.assertTrue(data['price_per_unit'] is None)
            del data['price_per_unit']
        self.assertTrue(all(isinstance(v, float) for v in data.values()))


class KrakenTests(unittest.TestCase):
    def setUp(self):
        self.api = KrakenTestAPI()

    def test_balances(self):
        balances = self.api.balances(currencies=['BTC', 'XRP'])
        self.assertTrue(set(balances.keys()) == set(['BTC', 'XRP']))
        self.assertTrue(all(isinstance(v, float) for v in balances.values()))

    def test_buy(self):
        uuid = self.api.buy(currency="XLM", size=0, rate=0.5)
        self.assertTrue(isinstance(uuid, basestring))

    def test_sell(self):
        uuid = self.api.sell(currency="XLM", size=0, rate=0.5)
        self.assertTrue(isinstance(uuid, basestring))

    def test_cancel(self):
        status = self.api.cancel(uuid="abc")
        self.assertTrue(status)

    def test_withdraw(self):
        uuid = self.api.withdraw(currency="XLM", size=0, destination="Bittrex")
        self.assertTrue(isinstance(uuid, basestring))

    def test_get_order(self):
        data = self.api.get_order(uuid="abc")
        required_keys = ['open_size', 'fill_size', 'price_per_unit']
        self.assertTrue(all(k in data for k in required_keys))
        if data['fill_size'] == 0:
            self.assertTrue(data['price_per_unit'] is None)
            del data['price_per_unit']
        self.assertTrue(all(isinstance(v, float) for v in data.values()))
