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
        self.coin: str = coin
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

    def get_exchange_info(self):
        headers = self.get_headers()
        url = self.host + self.uri
        logger.info(f"get {self.coin} info url: {url}")
        res = requests.get(url, headers=headers, timeout=10)
        retry = 10
        while res.status_code != 200 and retry > 0:
            sleep(1)
            self.info_date -= datetime.timedelta(1)
            self.info_date = self.info_date.replace(hour=23, minute=59, second=59, microsecond=0)
            self.uri = f"/v1/markets/balance?net={self.coin}&day={self.info_date.strftime('%Y%m%d')}"
            url = self.host + self.uri
            headers = self.get_headers()
            res = requests.get(url, headers=headers, timeout=10)
            retry -= 1

        balance_info = res.json()
        peckshield_balance_info = balance_info.get('result', [])
        self.balance_info = list(peckshield_balance_info)

        retry = 10
        res = requests.get('https://open.chain.info/v1/entity/list', timeout=10)
        while res.status_code != 200 and retry > 0:
            sleep(1)
            res = requests.get('https://open.chain.info/v1/entity/list', timeout=10)
            retry -= 1

        entity_list = res.json().get("data", {}).get('entityList', [])
        if not entity_list:
            return

        self._merge_info(entity_list)

    def parse_store_data(self):
        sql = """INSERT INTO balance_of_exchange_history (exchange, chain, coin, balance, source, create_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        for info in self.balance_info:
            exchange = info['entity']
            exchange = get_common_exchange_name(exchange)
            balance = info['balance']
            logger.info(f"exchange: {exchange}, coin: {self.coin}, balance: {balance}")
            execute_sql(sql, (exchange, "multiple", self.coin, balance, "peckshield", self.info_date))


def main():
    host = "https://api.peckshield.cn"
    user = "bituniverse"
    password = "yAzeiRhtwi40"
    btc_collector = BalanceCollector(host, user, password, "btc")
    btc_collector.get_exchange_info()
    btc_collector.parse_store_data()

    eth_collector = BalanceCollector(host, user, password, "eth")
    eth_collector.get_exchange_info()
    eth_collector.parse_store_data()


if __name__ == '__main__':
    main()
