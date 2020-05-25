import datetime
import logging
import argparse
from utils.log_util import setup_logging

setup_logging()
logger = logging.getLogger()

import re
from decimal import Decimal
from utils.common_util import retry

import requests

from utils.db_helper import execute_sql, read_all_from_db


data_js_pattern_usdt = r'.*var plotData2ab \= eval\(\[(.*),\]\);'
data_js_pattern_eth = r'.*var plotData \= eval\(\[(.*)\]\);'
items_pattern_usdt = r'\[Date\.UTC\((?P<year>[0-9]{4}),(?P<month>[0-9]{1,2}),(?P<day>[0-9]{1,2})\),(?:[0-9]+,){5}(?P<balance>[0-9]+(?:\.[0-9]+)?)'
items_pattern_eth = r'\[Date\.UTC\((?P<year>[0-9]{4}),(?P<month>[0-9]{1,2}),(?P<day>[0-9]{1,2})\),(?P<balance>[0-9]+(?:\.[0-9]+)?)'

balance_sql = """
    INSERT INTO balance_of_address_history(address, coin, balance, source, create_time)
    VALUES (%s, %s, %s, %s, %s)
"""
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}


@retry(requests.exceptions.RequestException, logger=logger)
def get_history_for_address(address, currency):
    if currency == 'usdt':
        url = f'https://etherscan.io/token/token-analytics?m=normal&contractAddress=0xdac17f958d2ee523a2206206994597c13d831ec7&a={address}&lg=en'
    else:
        url = f'https://etherscan.io/address-analytics?m=normal&a={address}&lg=en&cc=USD'
    charts_page = requests.get(url, headers=headers)

    data = []

    if charts_page.status_code != 200:
        logger.error("no datas found for address: {}, status: {}".format(address, charts_page.status_code))
        logger.error(charts_page.content)
        return data
    if currency == 'usdt':
        data_lines = re.findall(data_js_pattern_usdt, charts_page.content.decode(), re.I)
    else:
        data_lines = re.findall(data_js_pattern_eth, charts_page.content.decode(), re.I)
    if data_lines:
        data_str = data_lines[0]
        if currency == 'usdt':
            items_data = re.findall(items_pattern_usdt, data_str, re.I)
        else:
            items_data = re.findall(items_pattern_eth, data_str, re.I)
        logger.info(items_data)

        new_items = []
        for item_data in items_data:
            year, month, day, balance = item_data
            new_items.append((year, str(int(month)+1), day, balance))

        data = [{
            'date': datetime.datetime.strptime('-'.join(d[:3]), '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=0),
            'balance': Decimal(d[3])
        } for d in new_items]

    if not data:
        logger.error("no datas found for address: {}, status: {}".format(address, charts_page.status_code))
        return []

    data.sort(key=lambda d: d['date'])
    # 获取11.1之后的数据，如果没有11.1的数据则使用之前的一个点的数据
    start_date = datetime.datetime.strptime('2019-11-01', '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=0)
    for index, item in enumerate(data):
        if item['date'] == start_date:
            data = data[index:]
            break
        elif item['date'] > start_date and index != 0:
            data = data[index-1:]
            data[0]['date'] = start_date  # 还需要把日期改为11.1
            break

    # 如果还没有11.1的数据说明所有的数据都早于11.1，那么就取最后一条数据作为data的数据
    if data[0]['date'] != start_date:
        data = [data[-1]]
        data[0]['date'] = start_date  # 还需要把日期改为11.1

    return data


def update_balances(address, currency, exchange=None):
    datas = get_history_for_address(address, currency)
    if not datas:
        return

    current_date = datas[0]['date']
    current_balance = datas[0]['balance']
    max_date = datetime.datetime.now()
    current_index = 0
    max_length = len(datas)
    while current_date < max_date:
        current_item = datas[current_index]
        if current_item['date'] == current_date:
            current_balance = current_item['balance']
            if current_index < max_length - 1:
                current_index += 1
        execute_sql(
            balance_sql,
            (address, currency, current_balance, "etherscan", current_date))
        current_date += datetime.timedelta(1)


def main():
    parser = argparse.ArgumentParser(description='get history of usdt or eth balances')
    parser.add_argument(
        '--currency', '-c', action='store',
        help='specify currency type', type=str)
    parser.add_argument(
        '--exchange', '-e', action='store',
        help='specify exchange which you want to update history for', type=str)
    args = parser.parse_args()

    currency = args.currency or 'usdt'
    exchange = args.exchange

    if exchange:
        sql = 'select address from exchange_chain_address where exchange=%s'
        addresses = read_all_from_db(sql, (exchange,))
    else:
        sql = 'select address from exchange_chain_address'
        addresses = read_all_from_db(sql)

    total_count = len(addresses)
    logger.info('address count: {}'.format(total_count))
    index = 0
    for address_data in addresses:
        index += 1
        logger.info("progress: {}/{}".format(index, total_count))
        update_balances(address_data['address'], currency, exchange)


if __name__ == '__main__':
    main()
