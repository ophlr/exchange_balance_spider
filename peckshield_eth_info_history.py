#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
peckshield_eth_info_history.py
~~~~~~~~~~~~
get eth history info of exchanges from peckshield.

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

    # def _merge_eth_info(self, entity_list):
    #     exchange_set = {info['entity'] for info in self.eth_balance_info}
    #     for item in entity_list:
    #         exchange = item.get('name').lower()
    #         exchange_common_name = get_common_exchange_name(exchange)
    #         if exchange not in exchange_set and exchange_common_name not in exchange_set:
    #             self.eth_balance_info.append(
    #                 {"entity": exchange_common_name,
    #                  "balance": item.get("balance", 0)})

    def get_eth_info_history(self, days=42):
        while days > 0:
            self.info_date = self.info_date.replace(hour=23, minute=59, second=59, microsecond=0)
            self.uri = f"/v1/markets/balance?net=eth&day={self.info_date.strftime('%Y%m%d')}"
            url = self.host + self.uri
            logger.info(f"request url: {url}")
            headers = self.get_headers()
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                logger.warning(f'response status code {res.status_code}')
                self.info_date -= datetime.timedelta(1)
                days -= 1
                continue
            eth_balance_info = res.json()
            peckshield_eth_balance_info = eth_balance_info.get('result', [])
            self.eth_balance_info = list(peckshield_eth_balance_info)
            self.parse_store_data()
            self.info_date -= datetime.timedelta(1)
            days -= 1
            sleep(1)

    def parse_store_data(self):
        delete_sql = "DELETE FROM balance_of_exchange_history WHERE exchange=%s and date(create_time)=%s"
        insert_sql = "INSERT INTO balance_of_exchange_history (exchange, chain, coin, balance, source, create_time) VALUES (%s, %s, %s, %s, %s, %s)"
        for info in self.eth_balance_info:
            exchange = info['entity']
            exchange = get_common_exchange_name(exchange)
            eth_balance = info['balance']
            logger.info(
                f"insert into exchange_eth_balances table with "
                f"exchange<{exchange}> and eth balance<{eth_balance}>")
            # delete old data first, then insert new data
            execute_sql(delete_sql, (exchange, self.info_date.date()))
            execute_sql(insert_sql, (exchange, "multiple", "ETH", eth_balance, "peckshield", self.info_date))


def main():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"
    eth_collector = EthBalanceCollector(host, user, password)
    eth_collector.get_eth_info_history()


def test():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"
    c = EthBalanceCollector(host, user, password)
    h = c.get_headers()
    url = c.host + "/v1/markets/balance?net=eth&day=20191020"
    result = requests.get(url, headers=h)
    print(result)


if __name__ == '__main__':
    main()



