#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
peckshield_eth_info.py
~~~~~~~~~~~~
get eth info of exchanges from peckshield everyday.

:copyright: (c) 2019 by Ziqian Liu.

"""
import requests
import logging
import datetime
from time import sleep
from apiclient import ApiClient
from utils.db_helper import execute_sql
from utils.log_util import setup_logging
from utils.common_util import get_common_exchange_name

# EXCHANGE_NAME_MAPPING = {
#     'huobi': 'huobi.pro',
#     'bancor': 'bancor.network',
#     'yobit.net': 'yobit',
#     'zb.com': 'zb',
#     'coinexchange.io': 'coinexchange',
#     "bigone": 'big.one'
# }

setup_logging()
logger = logging.getLogger()


class EthBalanceCollector(ApiClient):
    def __init__(self, host, user, password):
        super(EthBalanceCollector, self).__init__(host, user, password)
        self.info_date = self.now
        self.uri = f"/v1/markets/balance?net=eth&day={self.info_date.strftime('%Y%m%d')}"

    def get_exchange_eth_info(self):
        headers = self.get_headers()
        url = self.host + self.uri
        logger.info(f"get eth info url: {url}")
        res = requests.get(url, headers=headers, timeout=10)
        retry = 10
        while res.status_code != 200 and retry > 0:
            sleep(1)
            self.info_date -= datetime.timedelta(1)
            self.info_date = self.info_date.replace(hour=23, minute=59, second=59, microsecond=0)
            self.uri = f"/v1/markets/balance?net=eth&day={self.info_date.strftime('%Y%m%d')}"
            url = self.host + self.uri
            headers = self.get_headers()
            res = requests.get(url, headers=headers, timeout=10)
            retry -= 1

        eth_balance_info = res.json()
        peckshield_eth_balance_info = eth_balance_info.get('result', [])
        self.eth_balance_info = list(peckshield_eth_balance_info)

    def parse_store_data(self):
        sql = """INSERT INTO balance_of_exchange_history (exchange, chain, coin, balance, source, create_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for info in self.eth_balance_info:
            exchange = info['entity']
            exchange = get_common_exchange_name(exchange)
            # if exchange.lower() == 'bitflyer':
            #     continue
            eth_balance = info['balance']
            logger.info(
                f"insert into exchange_eth_balances table with "
                f"exchange<{exchange}> and eth balance<{eth_balance}>")
            execute_sql(sql, (exchange, "multiple", "ETH", eth_balance, "peckshield", self.info_date))


def main():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"
    eth_collector = EthBalanceCollector(host, user, password)
    eth_collector.get_exchange_eth_info()
    eth_collector.parse_store_data()


if __name__ == '__main__':
    main()