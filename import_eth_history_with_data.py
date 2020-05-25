import datetime
import pdb
import sys
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
    INSERT INTO exchange_balance(address, balance, currency, create_time)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE balance = %s
"""
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}


def get_history_with_data(charts_page):
    data = []
    data_lines = re.findall(data_js_pattern_eth, charts_page, re.I)
    if data_lines:
        data_str = data_lines[0]

        items_data = re.findall(items_pattern_eth, data_str, re.I)
        logger.info(items_data)

        new_items = []
        for item_data in items_data:
            year, month, day, balance = item_data
            new_items.append((year, str(int(month) + 1), day, balance))

        data = [{
            'date': datetime.datetime.strptime('-'.join(d[:3]), '%Y-%m-%d').replace(hour=23, minute=59, second=59, microsecond=0),
            'balance': Decimal(d[3])
        } for d in new_items]

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


def update_balances(address, html):
    datas = get_history_with_data(html)
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
            (address, current_balance, 'eth', current_date, current_balance))
        current_date += datetime.timedelta(1)


def main():
    file_name = sys.argv[1]
    address = sys.argv[2]
    with open(file_name, 'r', encoding='utf-8') as f:
        html = f.read()

    update_balances(address, html)


if __name__ == '__main__':
    main()
