#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
peckshield_btc_info_history_update.py
~~~~~~~~~~~~
get btc info of exchanges from peckshield.

:copyright: (c) 2019 by Ziqian Liu.

"""
import requests
import base64
import hmac
import logging
import hashlib
import datetime
from time import sleep
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


class ApiClient(object):
    def __init__(self, host, user=None, password=None):
        self.user = user
        self.password = password
        self.host = host
        self.now = datetime.datetime.now(datetime.timezone.utc)
        self.uri = None

    def _get_signature(self, uri, method='GET', content_md5=''):
        md5_password = hashlib.md5(self.password.encode("utf-8")).hexdigest()
        date = self.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
        sign_msg = "{}&{}&{}&{}".format(method, uri, date, content_md5)
        signature = base64.b64encode(
            hmac.new(
                md5_password.encode("utf-8"), sign_msg.encode("utf-8"),
                digestmod=hashlib.sha1
            ).digest()
        ).decode()
        return signature

    def _get_auth_token(self):
        if not self.uri:
            raise NotImplementedError(
                "ApiClient shall be implemented and the self.uri shall be assigned")
        signature = self._get_signature(self.uri)
        return "PeckShield {}:{}".format(self.user, signature)

    def get_headers(self):
        auth = self._get_auth_token()
        date = self.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
        return {"Authorization": auth, "Date": date}


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

    def get_btc_info_history(self, days=42):
        while days > 0:
            self.info_date = self.info_date.replace(hour=23, minute=59, second=59, microsecond=0)
            self.uri = f"/v1/markets/balance?net=btc&day={self.info_date.strftime('%Y%m%d')}"
            url = self.host + self.uri
            logger.info(f"request url: {url}")
            headers = self.get_headers()
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                logger.warning(f'response status code {res.status_code}')
                self.info_date -= datetime.timedelta(1)
                days -= 1
                continue
            btc_balance_info = res.json()
            peckshield_btc_balance_info = btc_balance_info.get('result', [])
            self.btc_balance_info = list(peckshield_btc_balance_info)
            self.parse_store_data()
            self.info_date -= datetime.timedelta(1)
            days -= 1
            sleep(1)

    def parse_store_data(self):
        delete_sql = "DELETE FROM balance_of_exchange_history WHERE exchange=%s and date(create_time)=%s"
        insert_sql = "INSERT INTO balance_of_exchange_history (exchange, chain, coin, balance, source, create_time) VALUES (%s, %s, %s, %s, %s, %s)"
        for info in self.btc_balance_info:
            exchange = info['entity']
            exchange = get_common_exchange_name(exchange)
            btc_balance = info['balance']
            logger.info(
                f"insert into exchange_btc_balances table with "
                f"exchange<{exchange}> and btc balance<{btc_balance}>")
            # delete old data first, then insert new data
            execute_sql(delete_sql, (exchange, self.info_date.date()))
            execute_sql(insert_sql, (exchange, "multiple", "BTC", btc_balance, "peckshield", self.info_date))


def main():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"
    btc_collector = BtcBalanceCollector(host, user, password)
    btc_collector.get_btc_info_history()


if __name__ == '__main__':
    main()



