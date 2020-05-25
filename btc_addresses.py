import csv
import datetime
import logging
import requests
from utils.common_util import retry
# from configs.constants import DEFAULT_HEADERS
from utils.log_util import setup_logging

from utils.common_util import get_common_exchange_name
from utils.db_helper import execute_sql, read_all_from_db


setup_logging()
logger = logging.getLogger()


EXCHANGES = {}
with open('exchange_names.csv', 'r') as file:
    fieldnames = ['exchange', 'search_name']
    reader = csv.DictReader(file, fieldnames=fieldnames, delimiter=',')
    for row in reader:
        EXCHANGES[row['exchange']] = row['search_name']


addr_sql = """
        REPLACE INTO exchange_btc_address(address, exchange, tag)
        VALUES (%s, %s, %s)
    """

balance_sql = """
    INSERT INTO exchange_balance(address, balance, currency, create_time)
    VALUES (%s, %s, %s, %s)
"""

query_all_btc_addresses = """
select
    address
from
    exchange_btc_address
"""


def get_rank_data():
    body = {
        "from": 0,
        "name": "richestlist",
        "to": 10000
    }

    list_response = requests.post('https://api.chain.info/v2/stats/richestlist', json=body)

    if list_response.status_code != 200:
        logger.error("request btc address list error: {}".format(list_response.content))

    response_data = list_response.json()


    # with open('/Users/bing/Downloads/addresses.json', 'r') as file:
    #     response_data = json.load(file)

    list_data = response_data['data']['richList']
    time_stamp = response_data['data']['time']
    return list_data, time_stamp

def update_with_rank_data(list_data, time_stamp):
    total = len(list_data)
    index = 0

    for data in list_data:
        index += 1
        if index % 100 == 0:
            logger.info('progress: {}/{}'.format(index, total))

        tag_str = data.get('tag')
        address = data['address']

        if tag_str:
            tags = tag_str.split('-')
            exchange = tags[0]
            address_tag = '-'.join(tags[1:])
            exchange = get_common_exchange_name(exchange)
            execute_sql(addr_sql, [address, exchange, address_tag])
        else:
            continue
            # exchange = 'unknown'
            # address_tag = None

        balance = data['balance']
        update_time = datetime.datetime.fromtimestamp(time_stamp)

        execute_sql(balance_sql, [address, balance, 'btc', update_time])


def get_all_btc_addresses():
    all_addresses = read_all_from_db(query_all_btc_addresses)
    all_address_list = [item['address'] for item in all_addresses]
    return set(all_address_list)


@retry(requests.exceptions.RequestException, logger=logger)
def get_btc_balance_for_address(address):
    url = f'https://api.chain.info/v1/address/{address}'
    res = requests.get(url, timeout=10)
    if res.status_code != 200:
        logger.warning('response status code is %s' % res.status_code)
        raise requests.exceptions.RequestException()

    balance_info = res.json()
    return balance_info.get('balance') * pow(10, -8)


def update_non_rank_data(all_btc_addresses, rank_btc_addresses, time_stamp):
    update_time = datetime.datetime.fromtimestamp(time_stamp)
    non_rank_addresses = all_btc_addresses - rank_btc_addresses
    non_rank_address_no = len(non_rank_addresses)
    index = 1
    for address in non_rank_addresses:
        logger.info('processing %s/%s of non-rank addresses' % (index, non_rank_address_no))
        balance = get_btc_balance_for_address(address)
        logger.debug(f"insert non rank data: ({address}, {balance}, btc, {update_time})")
        execute_sql(balance_sql, (address, balance, 'btc', update_time))
        index += 1

    logger.info("complete non rank data update.")


def get_rank_addresses(list_data):
    return set([item['address'] for item in list_data])


def main():
    list_data, time_stamp = get_rank_data()
    update_with_rank_data(list_data, time_stamp)
    rank_addresses = get_rank_addresses(list_data)
    all_btc_addresses = get_all_btc_addresses()
    update_non_rank_data(all_btc_addresses, rank_addresses, time_stamp)


if __name__ == '__main__':
    main()
