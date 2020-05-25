import requests
import logging
import datetime

from time import sleep

from apiclient import ApiClient
from utils.db_helper import execute_sql
from utils.log_util import setup_logging
from utils.common_util import get_common_exchange_name

# setup_logging()
logger = logging.getLogger()


class BalanceCollector(ApiClient):
    def __init__(self, host, user, password, coin):
        super(BalanceCollector, self).__init__(host, user, password)
        self.info_date = self.now
        self.coin = coin
        self.uri = f"/v1/markets/balance?net={self.coin}&day={self.info_date.strftime('%Y%m%d')}"

    def _merge_info(self, entity_list):
        exchange_set = {info['entity'] for info in self.balance_info}
        for item in entity_list:
            exchange = item.get('name').lower()
            exchange_common_name = get_common_exchange_name(exchange)
            if exchange not in exchange_set and exchange_common_name not in exchange_set:
                self.balance_info.append(
                    {"entity": exchange_common_name,
                     "balance": item.get("balance", 0)})

    def get_info_history(self, days=42):
        while days > 0:
            self.info_date = self.info_date.replace(hour=23, minute=59, second=59, microsecond=0)
            self.uri = f"/v1/markets/balance?net={self.coin}&day={self.info_date.strftime('%Y%m%d')}"
            url = self.host + self.uri
            logger.info(f"request url: {url}")
            headers = self.get_headers()
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                logger.warning(f'response status code {res.status_code}')
                self.info_date -= datetime.timedelta(1)
                days -= 1
                continue
            balance_info = res.json()
            peckshield_balance_info = balance_info.get('result', [])
            self.balance_info = list(peckshield_balance_info)
            self.parse_store_data()
            self.info_date -= datetime.timedelta(1)
            days -= 1
            sleep(1)

    def parse_store_data(self):
        delete_sql = "DELETE FROM balance_of_exchange_history WHERE exchange=%s and date(create_time)=%s"
        insert_sql = "INSERT INTO balance_of_exchange_history (exchange, chain, coin, balance, source, create_time) VALUES (%s, %s, %s, %s, %s, %s)"
        for info in self.balance_info:
            exchange = info['entity']
            exchange = get_common_exchange_name(exchange)
            balance = info['balance']
            execute_sql(delete_sql, (exchange, self.info_date.date()))
            execute_sql(insert_sql, (exchange, "multiple", self.coin, balance, "peckshield", self.info_date))


def main():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"

    btc_collector = BalanceCollector(host, user, password, "btc")
    btc_collector.get_info_history()

    eth_collector = BalanceCollector(host, user, password, "eth")
    eth_collector.get_info_history()


if __name__ == '__main__':
    main()
