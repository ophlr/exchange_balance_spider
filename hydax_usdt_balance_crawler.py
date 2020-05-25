#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
hydax_usdt_balance_crawler.py
~~~~~~~~~~~~
craw hydax usdt balance individually.

:copyright: (c) 2019 by Ziqian Liu.

"""
import requests
import logging
import MySQLdb
import datetime
from lxml import html
from decimal import Decimal
from utils.common_util import retry
from utils.db_helper import execute_sql


logger = logging.getLogger(__name__)


# USDT address use the same table as ETH address table
addr_sql = """
    INSERT INTO exchange_chain_address(exchange, chain, address, tag, source, modify_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE exchange=VALUES(excgange), chain=VALUES(chain), tag=VALUES(tag), source=VALUES(source), modify_time=VALUES(modify_time)
"""

balance_sql = """
    INSERT INTO balance_of_address_history(address, coin,  balance, source, create_time)
    VALUES (%s, %s, %s, %s, %s)
"""


HYDAX_ADDR = "3Bm99oWTc7Eebrv4TEUBTY86t4AFXSdPRK"


def add_hydax_address():
    try:
        now = datetime.datetime.utcnow()
        execute_sql(addr_sql, ('hydax', "BTC", HYDAX_ADDR, 'hydax', 'tokenview', now))
    except MySQLdb._exceptions.IntegrityError:
        pass


@retry(requests.exceptions.RequestException, logger=logger)
def get_hydax_usdt_balance():
    url = "https://tokenview.com/en/search/3Bm99oWTc7Eebrv4TEUBTY86t4AFXSdPRK"
    res = requests.get(url, timeout=5)
    if not res or res.status_code != 200:
        logger.error(
            f"get balance for addr: {url} failed "
            f"with status code: {res.status_code}")
        return None

    res.encoding = 'utf-8'

    tree = html.fromstring(res.text)
    usdt_balance = tree.xpath('//*[@id="__layout"]/div/div[2]/div[4]/section/div[2]/div/div[1]/span[2]/text()')
    usdt_balance = Decimal(usdt_balance[0].strip(' \n'))
    return usdt_balance


def save_balance(balance):
    now = datetime.datetime.utcnow()
    execute_sql(balance_sql, [HYDAX_ADDR, "USDT", balance, 'tokenview', now])


if __name__ == '__main__':
    # add_hydax_address()
    usdt_balance = get_hydax_usdt_balance()
    save_balance(usdt_balance)

