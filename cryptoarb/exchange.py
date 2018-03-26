from abc import ABCMeta, abstractmethod
import json

from bittrex.bittrex import Bittrex as _Bittrex
from krakenex import API as _Kraken

from util import (initialize_logger, log_event, ClientError, concatMap)

with open("deposit_addresses.json") as f:
    deposit_addrs = json.load(f)


class AbstractExchange:
    __metaclass__ = ABCMeta

    @abstractmethod
    def markets(self):
        pass

    @abstractmethod
    def tickers(self, currencies):
        pass

    @abstractmethod
    def balances(self, currencies):
        pass

    @abstractmethod
    def buy(self, currency, size, rate):
        pass

    @abstractmethod
    def sell(self, currency, size, rate):
        pass

    @abstractmethod
    def cancel(self, uuid):
        pass

    @abstractmethod
    def withdraw(self, currency, size, destination):
        pass

    @abstractmethod
    def get_order(self, uuid):
        pass

    def asks(self, currencies):
        rates = self.tickers(currencies=currencies)
        return {k: v['ask'] for k, v in rates.items()}

    def bids(self, currencies):
        rates = self.tickers(currencies=currencies)
        return {k: v['bid'] for k, v in rates.items()}

    def lasts(self, currencies):
        rates = self.tickers(currencies=currencies)
        return {k: v['last'] for k, v in rates.items()}


class Bittrex(AbstractExchange):
    name = 'Bittrex'

    def __init__(self):
        with open("key/bittrex.json") as secrets_file:
            secrets = json.load(secrets_file)
        self.client = _Bittrex(
            api_key=secrets['key'], api_secret=secrets['secret'])
        self.logger = initialize_logger(self.__class__.__name__.upper())

    @log_event
    def markets(self):
        resp = self.client.get_markets()
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif resp['success'] and not resp['result']:
            raise Exception('Empty response from server')
        elif not resp['success']:
            raise ClientError(resp)
        return resp['result']

    @log_event
    def tickers(self, currencies):
        rates = {}

        for c in currencies:
            resp = self.client.get_ticker(market='BTC-%s' % c)
            if resp['message'] == 'NO_API_RESPONSE':
                raise Exception('No response from server')
            elif resp['success'] and not resp['result']:
                raise Exception('Empty response from server')
            elif not resp['success']:
                raise ClientError(resp)
            rates[c] = {k.lower(): v for k, v in resp['result'].items()}

        return rates

    @log_event
    def balances(self, currencies):
        resp = self.client.get_balances()
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif resp['success'] and not resp['result']:
            raise Exception('Empty response from server')
        elif not resp['success']:
            raise ClientError(resp)

        balances = {
            bal['Currency']: bal['Available']
            for bal in resp['result'] if bal['Currency'] in currencies
        }

        if not balances:
            self.logger.debug(resp)
            balances = self.balances(currencies=currencies)
            if not balances:
                raise Exception("Cannot get balances.")
        return balances

    @log_event
    def buy(self, currency, size, rate):
        resp = self.client.buy_limit(
            market='BTC-' + currency, quantity=size, rate=rate)
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif resp['success'] and not resp['result']:
            raise Exception('Empty response from server')
        elif not resp['success']:
            raise ClientError(resp)
        return resp['result']['uuid']

    @log_event
    def sell(self, currency, size, rate):
        resp = self.client.sell_limit(
            market='BTC-' + currency, quantity=size, rate=rate)
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif resp['success'] and not resp['result']:
            raise Exception('Empty response from server')
        elif not resp['success']:
            raise ClientError(resp)
        return resp['result']['uuid']

    @log_event
    def cancel(self, uuid):
        resp = self.client.cancel(uuid=uuid)
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif not resp['success']:
            raise ClientError(resp)
        return resp['success']

    @log_event
    def withdraw(self, currency, size, destination):
        resp = self.client.withdraw(
            currency=currency,
            quantity=size,
            address=deposit_addrs[destination][currency]['address'],
            paymentid=deposit_addrs[destination][currency]['memo'])
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif resp['success'] and not resp['result']:
            raise Exception('Empty response from server')
        elif not resp['success']:
            raise ClientError(resp)
        return resp['result']['uuid']

    @log_event
    def get_order(self, uuid):
        resp = self.client.get_order(uuid=uuid)
        if resp['message'] == 'NO_API_RESPONSE':
            raise Exception('No response from server')
        elif resp['success'] and not resp['result']:
            raise Exception('Empty response from server')
        elif not resp['success']:
            raise ClientError(resp)
        result = resp['result']
        data = {
            'open_size': result['QuantityRemaining'],
            'fill_size': result['Quantity'] - result['QuantityRemaining'],
            'price_per_unit': result['PricePerUnit']
        }

        return data


class Kraken(AbstractExchange):
    name = 'Kraken'

    def __init__(self):
        with open("key/kraken.json") as secrets_file:
            secrets = json.load(secrets_file)
        self.client = _Kraken(key=secrets['key'], secret=secrets['secret'])
        self.logger = initialize_logger(self.__class__.__name__.upper())

    @log_event
    def markets(self):
        resp = self.client.query_public(method="AssetPairs")
        if resp['error']: raise ClientError(resp['error'])

        return resp['result']

    @log_event
    def tickers(self, currencies):
        mapping = {'a': 'ask', 'b': 'bid', 'c': 'last'}
        pairs = ','.join(map(lambda x: 'X%sXXBT' % x, currencies))
        resp = self.client.query_public(method="Ticker", req={'pair': pairs})
        if resp['error']: raise ClientError(resp['error'])

        return {
            k[1:4]:
            {mapping[sk]: float(v[sk][0])
             for sk in v if sk in mapping}
            for k, v in resp['result'].items()
        }

    @log_event
    def balances(self, currencies):
        resp = self.client.query_private(method="Balance")
        if resp['error']: raise ClientError(resp['error'])

        self.logger.debug(resp)
        bals = resp['result']

        return {
            c: float(
                bals.get('XXBT', 0) if c == 'BTC' else bals.get('X%s' % c, 0))
            for c in currencies
        }

    @log_event
    def buy(self, currency, size, rate):
        resp = self.client.query_private(
            method="AddOrder",
            req={
                'pair': 'X%sXXBT' % currency,
                'type': 'buy',
                'ordertype': 'limit',
                # X%sXXBT price can only be specified up to 8 decimals.
                'price': '{:.8f}'.format(rate),
                'volume': '{:.0f}'.format(size)
            })
        if resp['error']: raise ClientError(resp['error'])

        self.logger.debug(resp)
        return ','.join(resp['result']['txid'])

    @log_event
    def sell(self, currency, size, rate):
        resp = self.client.query_private(
            method="AddOrder",
            req={
                'pair': 'X%sXXBT' % currency,
                'type': 'buy',
                'ordertype': 'limit',
                # X%sXXBT price can only be specified up to 8 decimals.
                'price': '{:.8f}'.format(rate),
                'volume': '{:.0f}'.format(size)
            })
        if resp['error']: raise ClientError(resp['error'])

        self.logger.debug(resp)
        return ','.join(resp['result']['txid'])

    @log_event
    def cancel(self, uuid):
        resp = self.client.query_private(
            method="CancelOrder", req={
                'txid': uuid
            })
        if resp['error']: raise ClientError(resp['error'])

        self.logger.debug(resp)
        return True

    @log_event
    def withdraw(self, currency, size, destination):
        resp = self.client.query_private(
            method="Withdraw",
            req={
                'asset': 'X%s' % currency,
                'key': deposit_addrs[destination][currency]['kraken_key'],
                'amount': '{:.0f}'.format(size)
            })
        if resp['error']: raise ClientError(resp['error'])

        self.logger.debug(resp)
        return resp['result']['refid']

    @log_event
    def get_order(self, uuid):
        resp = self.client.query_private(
            method="QueryOrders", req={
                'txid': uuid
            })
        if resp['error']: raise ClientError(resp['error'])

        self.logger.debug(resp)
        txs = concatMap(resp['result'].values(), lambda x: x.values())

        order_size = sum(float(tx['vol']) for tx in txs)
        fill_size = sum(float(tx['vol_exec']) for tx in txs)
        cost = sum(float(tx['cost']) for tx in txs)
        price_per_unit = cost / fill_size if fill_size else None

        data = {
            'open_size': order_size - fill_size,
            'fill_size': fill_size,
            'price_per_unit': price_per_unit
        }

        return data
