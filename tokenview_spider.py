import requests
import logging
import datetime
import time

from lxml import html
from decimal import Decimal
from utils.common_util import retry
from utils.db_helper import execute_sql, read_all_from_db

logger = logging.getLogger(__name__)

exchange_chain_address_sql = """
    INSERT INTO exchange_chain_address(exchange, chain, address, tag, source, modify_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE exchange=VALUES(excgange), chain=VALUES(chain), tag=VALUES(tag), source=VALUES(source), modify_time=VALUES(modify_time)
"""

balance_of_address_history_sql = """
    INSERT INTO balance_of_address_history(address, coin,  balance, source, create_time)
    VALUES (%s, %s, %s, %s, %s)
"""

balance_of_exchange_history = """
    INSERT INTO balance_of_exchange_history(exchange, chain, coin, balance, source)
    VALUES (%s, %s, %s, %s, %s)
"""

HYDAX_ADDR = [
    "3Bm99oWTc7Eebrv4TEUBTY86t4AFXSdPRK"
]

USDT_TOKEN_VIEW_ADDRESSES = [
    '1PktPwDM1h85GW4ab7Xgv83TALXJ71rXLt',
    '1CoC5neN7NgDyzVfzR3JSSusJSZz2QnAds',
    '3FVrw3oPAfAyqiyvBaJLKPwBUjW9eQjLsM'
]

TRX_TOKEN_VIEW_ADDRESSES = [
    'TJ1dgTTKgJ3do3ftjhKhSYYtMk1QeVWUay',
    'TJkQSMi5NQHSr2mFxkbtpaxyqdbK5PgzLd'
]

USDT_ERC_20_ADDRESSES = [
    '0x6465349f1a53ba0097d9aac3f6ef293bdd10cae1',
    '0x28c9386eBab8D52Ead4A327e6423316435B2d4fc',
    '0x4bd671825810dd7a27d73185a79e095924f2aa12'
]

BTC_TOKEN_VIEW_ADDRESSES = [
    '1PktPwDM1h85GW4ab7Xgv83TALXJ71rXLt',
    '1CoC5neN7NgDyzVfzR3JSSusJSZz2QnAds',
    '3FVrw3oPAfAyqiyvBaJLKPwBUjW9eQjLsM',
    '34izf8aHhRN56AS6MpiE3oYzHdSR2xVHkx',
    '1ML777mfvjwDazbiDgzKoQfz1rpkwMbGP3',
    '3HmJDpQvyqfZ2RZvyheB43NBPQRN3FVoFd'

]

ETH_ERC_20_ADDRESSES = [
    '0x6465349f1a53ba0097d9aac3f6ef293bdd10cae1',
    '0x28c9386eBab8D52Ead4A327e6423316435B2d4fc',
    '0x69166e49d2fd23E4cbEA767d7191bE423a7733A5',
    '0x4bd671825810dd7a27d73185a79e095924f2aa12'
]

HBTC_BTC_ADDRESSES = [
    '378E5HP7SGcW6XvriEirgvNUF54G79mftB',
    '3CTgNAMCY6ZoMFYPNFo8xBLeVv4stcXBhx'
]


def add_address():
    now = datetime.datetime.utcnow()
    for address in USDT_TOKEN_VIEW_ADDRESSES:
        print(f"add address: {address}")
        execute_sql(exchange_chain_address_sql, ('renrenbit', 'BTC', address, 'renrenbit usdt token', 'tokenview', now))
    for address in USDT_ERC_20_ADDRESSES:
        print(f"add address: {address}")
        execute_sql(exchange_chain_address_sql, ('renrenbit', 'ETH', address, 'renrenbit usdt erc20', 'tokenview', now))
    for address in ETH_ERC_20_ADDRESSES:
        print(f"add address: {address}")
        execute_sql(exchange_chain_address_sql, ('renrenbit', 'ETH', address, 'renrenbit eth erc20', 'tokenview', now))
    for address in BTC_TOKEN_VIEW_ADDRESSES:
        print(f"add address: {address}")
        execute_sql(exchange_chain_address_sql, ('renrenbit', 'BTC', address, 'renrenbit btc', 'tokenview', now))
    for address in HBTC_BTC_ADDRESSES:
        print(f"add address: {address}")
        execute_sql(exchange_chain_address_sql, ('hbtc', 'BTC', address, 'hbtc btc', 'tokenview', now))
    for address in HYDAX_ADDR:
        print(f"add address: {address}")
        execute_sql(exchange_chain_address_sql, ('hydax', "BTC", address, 'hydax', 'tokenview', now))


@retry(requests.exceptions.RequestException, logger=logger)
def get_balance_from_tokenview(address):
    res = requests.get(f"https://tokenview.com/en/search/{address}", timeout=10)
    if not res or res.status_code != 200:
        logger.error(
            f"get balance for addr: {address} failed "
            f"with status code: {res.status_code}")
        return None, None

    res.encoding = 'utf-8'
    tree = html.fromstring(res.text)
    btc_balances = tree.xpath('//span[text()="BTC"]/../../../../div[2]/div[1]/div[1]/span[2]/text()')
    if len(btc_balances) < 1:
        btc_balance = None
    else:
        btc_balance = btc_balances[0].strip()
    usdt_balances = tree.xpath('//span[text()="USDT"]/../../../../div[2]/div[1]/div[1]/span[2]/text()')
    if len(usdt_balances) < 1:
        usdt_balance = None
    else:
        usdt_balance = usdt_balances[0].strip()
    return Decimal(usdt_balance) if usdt_balance is not None else None, Decimal(
        btc_balance) if btc_balance is not None else None


def save_to_balance_of_address_history(address, coin, balance, source):
    if balance is not None:
        now = datetime.datetime.utcnow()
        execute_sql(balance_of_address_history_sql, [address, coin, balance, source, now])
    else:
        logger.warning(f"cannot save data addrees: {address} coin: {coin}, balance: {balance}")


def save_to_balance_of_exchange_history(exchange, chain, coin, balance, source):
    if balance is not None:
        execute_sql(balance_of_exchange_history, [exchange, chain, coin, balance, source])
    else:
        logger.warning(f"cannot save data chain: {chain} coin: {coin}, balance: {balance}")


def get_all_balance():
    addresses = read_all_from_db('select exchange, address from exchange_chain_address', [])

    for exchange, chain, address in addresses:
        usdt_balance, btc_balance = get_balance_from_tokenview(address)
        time.sleep(0.5)
        print(f"exchange: {exchange}, chain: {chain}, address: {address}")
        save_to_balance_of_address_history(address, "BTC", btc_balance, "tokenview")
        save_to_balance_of_address_history(address, "USDT", usdt_balance, "tokenview")
        save_to_balance_of_exchange_history(exchange, chain, "BTC", btc_balance, "tokenview")
        save_to_balance_of_exchange_history(exchange, chain, "USDT", usdt_balance, "tokenview")


if __name__ == '__main__':
    add_address()
    get_all_balance()
