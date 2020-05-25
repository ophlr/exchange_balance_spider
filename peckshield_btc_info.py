#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
peckshield_btc_info.py
~~~~~~~~~~~~
get btc info of exchanges from peckshield.

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


class BtcBalanceCollector(ApiClient):
    def __init__(self, host, user, password):
        super(BtcBalanceCollector, self).__init__(host, user, password)
        self.info_date = self.now
        self.uri = f"/v1/markets/balance?net=btc&day={self.info_date.strftime('%Y%m%d')}"

    def _merge_btc_info(self, entity_list):
        exchange_set = {info['entity'] for info in self.btc_balance_info}
        for item in entity_list:
            exchange = item.get('name').lower()
            exchange_common_name = get_common_exchange_name(exchange)
            if exchange not in exchange_set and exchange_common_name not in exchange_set:
                self.btc_balance_info.append(
                    {"entity": exchange_common_name,
                     "balance": item.get("balance", 0)})

    def get_exchange_btc_info(self):
        headers = self.get_headers()
        url = self.host + self.uri
        logger.info(f"get btc info url: {url}")
        res = requests.get(url, headers=headers, timeout=10)
        retry = 10
        while res.status_code != 200 and retry > 0:
            sleep(1)
            self.info_date -= datetime.timedelta(1)
            self.info_date = self.info_date.replace(hour=23, minute=59, second=59, microsecond=0)
            self.uri = f"/v1/markets/balance?net=btc&day={self.info_date.strftime('%Y%m%d')}"
            url = self.host + self.uri
            headers = self.get_headers()
            res = requests.get(url, headers=headers, timeout=10)
            retry -= 1

        btc_balance_info = res.json()
        peckshield_btc_balance_info = btc_balance_info.get('result', [])
        self.btc_balance_info = list(peckshield_btc_balance_info)

        retry = 10
        res = requests.get('https://open.chain.info/v1/entity/list', timeout=10)
        while res.status_code != 200 and retry > 0:
            sleep(1)
            res = requests.get('https://open.chain.info/v1/entity/list', timeout=10)
            retry -= 1

        entity_list = res.json().get("data", {}).get('entityList', [])
        if not entity_list:
            return

        self._merge_btc_info(entity_list)

    def parse_store_data(self):
        sql = """INSERT INTO balance_of_exchange_history (exchange, chain, coin, balance, source, create_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        # sql = "replace INTO exchange_btc_balances (exchange, balance, create_time) VALUES (%s, %s, %s)"
        for info in self.btc_balance_info:
            exchange = info['entity']
            exchange = get_common_exchange_name(exchange)
            # if exchange.lower() == 'bitflyer':
            #     continue
            btc_balance = info['balance']
            logger.info(
                f"insert into exchange_btc_balances table with "
                f"exchange<{exchange}> and btc balance<{btc_balance}>")
            execute_sql(sql, (exchange, "multiple", "BTC", btc_balance, "peckshield", self.info_date))


def main():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"
    btc_collector = BtcBalanceCollector(host, user, password)
    btc_collector.get_exchange_btc_info()
    btc_collector.parse_store_data()


if __name__ == '__main__':
    main()



