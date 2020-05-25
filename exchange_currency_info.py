#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
exchange_currency_info.py
~~~~~~~~~~~~
get exchange currency info from etherscan.io.

:copyright: (c) 2019 by Ziqian Liu.

"""
import logging
import requests
import re
import datetime
import MySQLdb
from utils.db_helper import execute_sql, read_all_from_db
from utils.common_util import Throttle, retry, get_decimal
from configs.constants import *
from utils.log_util import setup_logging
import csv

# setup_logging()
logger = logging.getLogger()

EXCHANGES = {}
with open('exchange_names.csv', 'r') as file:
    fieldnames = ['exchange', 'search_name']
    reader = csv.DictReader(file, fieldnames=fieldnames, delimiter=',')
    for row in reader:
        EXCHANGES[row['exchange']] = row['search_name']

CURRENCY_DECIMAL = {
    'usdt': 6,
    'eth': 18
}

CURRENCY_CONTRACT = {
    'usdt': '0xdac17f958d2ee523a2206206994597c13d831ec7'
}

throttle = Throttle(5)

addr_sql = """
    INSERT INTO exchange_chain_address(exchange, chain, address, tag, source)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE exchange=VALUES(excgange), chain=VALUES(chain), tag=VALUES(tag), source=VALUES(source)
"""
balance_sql = """
    INSERT INTO balance_of_address_history(address, coin, balance, source)
    VALUES (%s, %s, %s, %s)
"""

DEFAULT_REQUEST_HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
}


def get_re_pattern(exchange):
    pattern_str = '^%s( [0-9]+)?.*$' % exchange
    return re.compile(pattern_str)


@retry(requests.exceptions.RequestException, logger=logger)
def get_account_balance(exchange_addr, currency='usdt'):
    contract_addr = CURRENCY_CONTRACT[currency]
    delay = throttle.run()
    if delay:
        logger.debug('GET URL throttles. delay for %.2f' % (delay))
    url = f'https://api.etherscan.io/api?module=account&action=tokenbalance&' \
          f'contractaddress={contract_addr}&address={exchange_addr}&' \
          f'tag=latest&apikey={API_TOKEN}'
    res = requests.get(url, timeout=5)
    if not res or res.status_code != 200:
        logger.warning(
            f"get balance for addr: {exchange_addr} failed "
            f"with status code: {res.status_code}")
        return None
    body = res.json()
    if not body:
        raise ValueError(f'body is {body}')

    decimal_num = get_decimal(str(body['result']), CURRENCY_DECIMAL.get(currency))
    return decimal_num


@retry(requests.exceptions.RequestException, logger=logger)
def get_eth_balance(exchange_addr):
    delay = throttle.run()
    if delay:
        logger.debug('GET URL throttles. delay for %.2f' % (delay))
    url = f'https://api.etherscan.io/api?module=account&action=balance&address={exchange_addr}&tag=latest&apikey={API_TOKEN}'
    res = requests.get(url, timeout=5)
    if not res or res.status_code != 200:
        logger.warning(
            f"get balance for addr: {exchange_addr} failed "
            f"with status code: {res.status_code}")
        return None
    body = res.json()
    if not body:
        raise ValueError(f'body is {body}')

    decimal_num = get_decimal(str(body['result']), CURRENCY_DECIMAL.get('eth'))
    return decimal_num


@retry(requests.exceptions.RequestException, logger=logger)
def get_exchange_addresses(exchange, exchange_name):
    addr_list = set()
    delay = throttle.run()
    if delay:
        logger.debug('GET URL throttles. delay for %.2f' % (delay))
    url = f'https://etherscan.io/searchHandler?term={exchange}&filterby=3'
    res = requests.get(url, headers=DEFAULT_REQUEST_HEADERS, timeout=10)
    if not res or res.status_code != 200:
        logger.warning(
            f"get address for exchange<{exchange}> failed "
            f"with status code: {res.status_code}")
        return addr_list
    body = res.json()
    print("json------------->\n")
    print(res.text)
    print("text------------->\n")
    print(body)
    if body is None:
        raise ValueError(f'body is {body}')
    # exchange_pattern = get_re_pattern(exchange)
    pattern = re.compile('^(?P<tag>%s(?:(?: [0-9]+)|(?:: [a-z]+))?)\t(?P<address>0x[0-9a-f]{40}).*$' % exchange_name,
                         re.I)
    for addr in body:
        addr_list.update(pattern.findall(addr))
    return addr_list


def find_address():
    for exchange, search_name in EXCHANGES.items():
        address_list = set()
        address_list.update(get_exchange_addresses(search_name, search_name))
        # address_list.update(get_exchange_addresses(search_name + '%20', search_name))
        # address_list.update(get_exchange_addresses(search_name + ': Hot Wallet', search_name + ': Hot Wallet'))
        # address_list.update(get_exchange_addresses(search_name + ': Cold Wallet', search_name + ': Cold Wallet'))
        # for addr in address_list:
        #     logger.info(f"insert into exchange_eth_address with values: {addr[1]}, {exchange}, {addr[0]}")
        #     try:
        #         execute_sql(addr_sql, (exchange, "ETH", addr[1], addr[0], "etherscan"))
        #     except MySQLdb._exceptions.IntegrityError:
        #         logger.warning(f"database integrityerror, address: {addr}")
        #         continue


def find_balances():
    addresses = read_all_from_db('select address from exchange_chain_address', [])

    for address in addresses:
        addr = address['address']
        balance = get_account_balance(addr)
        if balance:
            logger.info(f"insert balance_of_address_history with values: {addr}, {balance}, usdt")
            execute_sql(balance_sql, (addr, "USDT", balance, "etherscan"))

        eth_balance = get_eth_balance(addr)
        if eth_balance:
            logger.info(f"insert exchange_balance with values: {addr}, {eth_balance}, eth")
            execute_sql(balance_sql, (addr, "ETH", balance, 'etherscan'))


if __name__ == "__main__":
    find_address()
    # find_balances()
