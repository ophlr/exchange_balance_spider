#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
common_util.py
~~~~~~~~~~~~
common utils.

:copyright: (c) 2019 by Ziqian Liu.

"""
import csv
import time
import os
from functools import wraps
from decimal import Decimal
from utils.db_helper import read_one_from_db


class Throttle(object):
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.rate_cnt = 0
        self.rate_ts = time.time()

    def set_rate_limit(self, v):
        self.rate_limit = v

    def run(self, inc=1):
        if self.rate_limit:
            now_ts = time.time()
            # use float to enable float division, so that the ts can be float instead of integer
            ts = self.rate_cnt / float(self.rate_limit)
            self.rate_cnt += inc
            delay = ts - (now_ts - self.rate_ts)
            if delay > 0.1:
                time.sleep(delay)
                return delay
        return 0


def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def get_decimal(number, decimals):
    """get decimal number

    :param number: a number string or int number
    :param decimals: decimals of the number
    :return: decimal type of the number
    :rtype: Decimal
    """
    if number == 0 or number == '0':
        return Decimal(0)

    number = str(number)
    if len(number) <= 6:
        # notice: to add 0 before number
        number = ((decimals - len(number) + 1) * '0') + number
    decimal_num = number[:-decimals] + '.' + number[-decimals:]
    return Decimal(decimal_num)


class Singleton(type):
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__new__(*args, **kwargs)
        return cls._instances[cls]


def get_updated_date(table, field='create_time'):
    """get the most recent date of record for specified table

    :param table: table name of database
    :param field: the field name you want to query, default is 'create_time'
    :return: the most recent date of the field you specified.
    :rtype: date
    """
    sql = f"SELECT max({field}) AS updated_time FROM {table}"
    updated_time = read_one_from_db(sql).get('updated_time')
    assert updated_time is not None, f"no {field} for table {table}"
    return updated_time.date()


EXCHANGES = {}
with open('exchange_names.csv', 'r') as file:
    fieldnames = ['exchange', 'search_name']
    reader = csv.DictReader(file, fieldnames=fieldnames, delimiter=',')
    for row in reader:
        EXCHANGES[row['exchange']] = row['search_name']


def get_common_exchange_name(exchange):
    exchange = exchange.lower()
    result = exchange
    for exchange_name, search_name in EXCHANGES.items():
        if exchange_name == exchange or search_name == exchange:
            result = exchange_name
    return result


def tester():
    # updated_date = get_updated_date('exchange_btc_balances')
    # print(updated_date)
    name = get_common_exchange_name('huobi')
    print(name)


if __name__ == '__main__':
    tester()
