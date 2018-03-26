import time
import datetime
import uuid
from functools import wraps, reduce
from operator import add
import logging
import requests


def initialize_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    handler = logging.FileHandler('log/%s' % now)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def log_event(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        event = "{}".format(f.__name__).upper()
        params = ', '.join(
            ['{}:{}'.format(str(k), repr(v)) for k, v in kwargs.items()])
        self.logger.debug(event + " - " + params)

        ex_msgs = [
            'No response from server', 'Empty response from server',
            'Connection reset by peer', 'No JSON object could be decoded',
            'The read operation timed out'
        ]

        uid = uuid.uuid4().int
        for i in xrange(5):
            try:
                out = f(self, *args, **kwargs)
                self.logger.debug('result:{}'.format(str(out)))
                return out
            except ClientError as ex:
                templ = "An exception with {} occurred. Arguments: {!r}"
                message = templ.format(type(ex), ex.args)
                self.logger.error(message)
                raise
            except Exception as ex:
                for msg in ex_msgs:
                    if msg in ex.args:
                        templ = "{} Attempt: {:d} UUID: {:d}"
                        message = templ.format(msg, i + 1, uid)
                        self.logger.warning(message)
                        time.sleep(5)
                        break
                else:
                    templ = "An exception with {} occurred. Arguments: {!r}"
                    message = templ.format(type(ex), ex.args)
                    self.logger.error(message)
                    raise
                continue
        raise Exception(uid)

    return wrapper


class ClientError(Exception):
    pass


def concatMap(xs, f):
    return reduce(add, map(f, xs), [])


def reverse_dict(data):
    from collections import defaultdict

    flipped = defaultdict(dict)
    for key, val in data.items():
        for subkey, subval in val.items():
            flipped[subkey][key] = subval

    return dict(flipped)


def merge_dicts(original, update):
    for key, value in original.iteritems():
        if key not in update:
            update[key] = value
        elif isinstance(value, dict):
            merge_dicts(value, update[key])
    return update


def usd_btc_rate():
    return float(
        requests.post('https://api.cryptonator.com/api/full/usd-btc').json()[
            'ticker']['price'])
