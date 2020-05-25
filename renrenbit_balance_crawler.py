#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
renrenbit_balance_crawler.py
~~~~~~~~~~~~
craw renrenbi balance individually.

:copyright: (c) 2019 by Ziqian Liu.

"""
import requests
import logging
import MySQLdb
import datetime
from time import sleep
from lxml import html
from decimal import Decimal
from utils.common_util import retry
from utils.db_helper import execute_sql


logger = logging.getLogger(__name__)


# USDT address use the same table as ETH address table
# insert_eth_addr_sql = """
#     INSERT INTO exchange_chain_address(exchange, chain, address, tag, source, modify_time)
#     VALUES (%s, %s, %s, %s, %s, %s)
#     ON DUPLICATE KEY UPDATE exchange=VALUES(exchange), chain=VALUES(chain), tag=VALUES(tag), source=VALUES(source), modify_time=VALUES(modify_time)
# """

insert_btc_addr_sql = """
    INSERT INTO exchange_chain_address(exchange, chain, address, tag, source, modify_time)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE exchange=VALUES(exchange), chain=VALUES(chain), tag=VALUES(tag), source=VALUES(source), modify_time=VALUES(modify_time)
"""

erc20_balance_sql = """
    INSERT INTO balance_of_address_history(address, coin,  balance, source)
    VALUES (%s, %s, %s, %s)
"""

btc_balance_sql = """
    INSERT INTO balance_of_exchange_history(exchange, chain, coin, balance, source)
    VALUES (%s, %s, %s, %s, %s)
"""

USDT_TOKEN_VIEW_ADDRESSES = [
    '1PktPwDM1h85GW4ab7Xgv83TALXJ71rXLt',
    '1CoC5neN7NgDyzVfzR3JSSusJSZz2QnAds',
    '3FVrw3oPAfAyqiyvBaJLKPwBUjW9eQjLsM'
]

TRX_TOKEN_VIEW_ADDRESSES = [
    'TJ1dgTTKgJ3do3ftjhKhSYYtMk1QeVWUay',
    'TJkQSMi5NQHSr2mFxkbtpaxyqdbK5PgzLd'
]

USDT_ERC_20_ADDRESSES = [
    '0x6465349f1a53ba0097d9aac3f6ef293bdd10cae1',
    '0x28c9386eBab8D52Ead4A327e6423316435B2d4fc',
    '0x4bd671825810dd7a27d73185a79e095924f2aa12'
]

BTC_TOKEN_VIEW_ADDRESSES = [
    '1PktPwDM1h85GW4ab7Xgv83TALXJ71rXLt',
    '1CoC5neN7NgDyzVfzR3JSSusJSZz2QnAds',
    '3FVrw3oPAfAyqiyvBaJLKPwBUjW9eQjLsM',
    '34izf8aHhRN56AS6MpiE3oYzHdSR2xVHkx',
    '1ML777mfvjwDazbiDgzKoQfz1rpkwMbGP3',
    '3HmJDpQvyqfZ2RZvyheB43NBPQRN3FVoFd'

]

ETH_ERC_20_ADDRESSES = [
    '0x6465349f1a53ba0097d9aac3f6ef293bdd10cae1',
    '0x28c9386eBab8D52Ead4A327e6423316435B2d4fc',
    '0x69166e49d2fd23E4cbEA767d7191bE423a7733A5',
    '0x4bd671825810dd7a27d73185a79e095924f2aa12'
]

HBTC_BTC_ADDRESSES = [
    '378E5HP7SGcW6XvriEirgvNUF54G79mftB',
    '3CTgNAMCY6ZoMFYPNFo8xBLeVv4stcXBhx'
]


# def add_erc_20_address():
#     try:
#         for address in USDT_TOKEN_VIEW_ADDRESSES:
#             execute_sql(insert_eth_addr_sql, ('renrenbit', 'BTC', address, 'renrenbit usdt token', 'tokenview', datetime.datetime.utcnow()))
#         for address in USDT_ERC_20_ADDRESSES:
#             execute_sql(insert_eth_addr_sql, ('renrenbit', 'ETH', address, 'renrenbit usdt erc20', 'tokenview', datetime.datetime.utcnow()))
#         for address in ETH_ERC_20_ADDRESSES:
#             execute_sql(insert_eth_addr_sql, ('renrenbit', 'ETH', address, 'renrenbit eth erc20', 'tokenview', datetime.datetime.utcnow()))
#         for address in BTC_TOKEN_VIEW_ADDRESSES:
#             execute_sql(insert_btc_addr_sql, ('renrenbit', 'BTC', address, 'renrenbit btc', 'tokenview', datetime.datetime.utcnow()))
#
#     except MySQLdb._exceptions.IntegrityError:
#         pass


def add_btc_address():
    try:
        for address in BTC_TOKEN_VIEW_ADDRESSES:
            execute_sql(insert_btc_addr_sql, ('renrenbit', 'BTC', address, 'renrenbit btc', 'tokenview', datetime.datetime.utcnow()))

    except MySQLdb._exceptions.IntegrityError:
        pass


@retry(requests.exceptions.RequestException, logger=logger)
def get_usdt_balance(address):
    url = f"https://tokenview.com/en/search/{address}"
    res = requests.get(url, timeout=5)
    if not res or res.status_code != 200:
        logger.error(
            f"get balance for addr: {url} failed "
            f"with status code: {res.status_code}")
        return None

    res.encoding = 'utf-8'

    tree = html.fromstring(res.text)
    usdt_balance = tree.xpath('//*[@id="__layout"]/div/div[2]/div[@class="mul-item"]//span[text()="USDT"]/../../../../div[2]/div/div[1]/span[2]/text()')
    if usdt_balance:
        return Decimal(usdt_balance[0].strip(' \n'))
    else:
        return None


@retry(requests.exceptions.RequestException, logger=logger)
def get_btc_balance(address):
    url = f"https://tokenview.com/en/search/{address}"
    res = requests.get(url, timeout=5)
    if not res or res.status_code != 200:
        logger.error(
            f"get balance for addr: {url} failed "
            f"with status code: {res.status_code}")
        return None

    res.encoding = 'utf-8'

    tree = html.fromstring(res.text)
    btc_balance = tree.xpath('//*[@id="__layout"]/div/div[2]/div[@class="mul-item"]//span[text()="BTC"]/../../../../div[2]/div/div[1]/span[2]/text()')
    btc_balance = Decimal(btc_balance[0].strip(' \n'))
    return btc_balance


def save_erc_20_balance(address, balance):
    execute_sql(erc20_balance_sql, [address, "USDT", balance, 'tokenview'])


def save_btc_balance_with_address(address, balance):
    execute_sql(erc20_balance_sql, [address, "BTC", balance, 'tokenview'])


def save_btc_balance(exchange, balance):
    execute_sql(btc_balance_sql, (exchange, "multiple", "BTC", balance, "tokenview"))


if __name__ == '__main__':
    # add_erc_20_address()
    add_btc_address()
    btc_balance = 0
    for address in USDT_TOKEN_VIEW_ADDRESSES:
        usdt_balance = get_usdt_balance(address)
        sleep(0.5)
        save_erc_20_balance(address, usdt_balance)

    for address in BTC_TOKEN_VIEW_ADDRESSES:
        balance_res = get_btc_balance(address)
        sleep(0.5)
        save_btc_balance_with_address(address, balance_res)
        btc_balance += balance_res

    save_btc_balance('renrenbit', btc_balance)

    btc_balance = 0
    for address in HBTC_BTC_ADDRESSES:
        balance_res = get_btc_balance(address)
        sleep(0.5)
        save_btc_balance_with_address(address, balance_res)
        btc_balance += balance_res

    save_btc_balance('hbtc', btc_balance)

    for address in HBTC_BTC_ADDRESSES:
        usdt_balance = get_usdt_balance(address)
        if not usdt_balance:
            continue
        sleep(0.5)
        save_erc_20_balance(address, usdt_balance)

