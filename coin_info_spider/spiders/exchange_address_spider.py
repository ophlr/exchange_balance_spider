#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
exchange_address_spider.py
~~~~~~~~~~~~
To scrape exchange's coin address.

:copyright: (c) 2019 by Ziqian Liu.

"""
import scrapy
import logging
import requests
import datetime
import MySQLdb
from utils.db_helper import execute_sql
from utils.common_util import Throttle, retry, get_decimal
from configs.constants import *
# from fake_useragent import UserAgent


logger = logging.getLogger(__name__)


EXCHANGES = [
    'abcc', 'abucoins', 'acx', 'aex', 'aidosmarket', 'allcoin', 'altcointrader',
    'arbitraging', 'artisturba', 'ataix', 'bancor.network', 'bcex', 'bgj.io',
    'bgogo', 'bhex', 'bibox', 'big.one', 'bilaxy', 'binance', 'binance.dex',
    'bisq', 'bit-z', 'bit2c', 'bitbank.cc', 'bitbay', 'bitbns', 'bitbox',
    'bitcoin.co.id', 'bitcoin.de', 'bitcointoyou', 'bitcointrade', 'bite.ceo',
    'bitebtc', 'bitex.la', 'bitfinex', 'bitflyer', 'bitforex', 'bitgrail',
    'bithesap', 'bithumb', 'bitinka', 'bitlish', 'bitmarket',
    'bitmart', 'bitmax-io', 'bitmex', 'bitoex', 'bitonic.nl', 'bitopro',
    'bitrabbit', 'bitsane', 'bitshares', 'bitso', 'bitstamp', 'bittrex',
    'bkex', 'bl3p.eu', 'bleutrade', 'blinktrade', 'braziliex', 'btcalpha',
    'btcbox', 'btcmarkets', 'btctrade.im', 'btcturk', 'btcxindia', 'buda',
    'bw', 'bx.in.th', 'bytex', 'c-cex', 'c2cx', 'cbx.one', 'ceo', 'cex.io',
    'chaoex', 'cobinhood', 'coinall', 'coinbase', 'coinbene', 'coinbig',
    'coincheck', 'coindcx', 'coineal', 'coinegg', 'coinex', 'coinexchange',
    'coinfalcon', 'coinfloor', 'coingeto', 'coinmate', 'coinmaxex',
    'coinnest', 'coinoah', 'coinone', 'coinpark.cc', 'coinroom', 'coins.ph',
    'coinsbank', 'coinspot', 'coinsquare', 'coinsuper', 'cointiger', 'coinw',
    'coolcoin', 'coss', 'cpdax', 'crex24', 'crossexchange', 'cryptobridge',
    'cryptonex', 'cryptopia', 'cryptox', 'ddex', 'dew', 'digifinex',
    'dragonex', 'dsx.uk', 'ethexindia', 'ethfinex', 'exmo', 'exx', 'ezbtc',
    'fargobase', 'fcoin', 'fex', 'fisco', 'forkdelta', 'gate.io', 'gatecoin',
    'gdac', 'gdax', 'gemini', 'getbtc', 'giottus', 'gopax', 'graviex', 'hadax',
    'hb.top', 'hcoin', 'hitbtc', 'hotbit', 'huobi', 'hydax', 'icoinbay',
    'idax', 'idcm', 'idex', 'independentr', 'indodax', 'infinitycoin', 'ioaex',
    'iquant', 'itbit', 'jex', 'joyso', 'kkcoin', 'koineks', 'koinex', 'koinim',
    'korbit', 'kraken', 'kryptono', 'kucoin', 'kuna', 'lakebtc', 'lbank',
    'liquid', 'litebit.eu', 'livecoin', 'luno', 'lykke', 'maicoin', 'max',
    'mercadobitcoin', 'mercadoniobiocash', 'mercatox', 'mxc', 'nanex',
    'negociecoins', 'neraex', 'nlexch', 'nocks', 'novaexchange', 'ok.top',
    'okcoinkr', 'okex', 'omicrex', 'ooobtc', 'otcbtc', 'ovis', 'palitanx',
    'paribu', 'paymium', 'pionex', 'poloniex', 'qtrade.io', 'quadrigacx',
    'remitano', 'rfinex', 'rightbtc', 'rootrex', 'rudex', 'satang', 'shubao',
    'simex', 'southxchange', 'stellarport', 'stellarterm', 'stex',
    'stocks.exchange', 'tdax', 'therocktrading', 'tidex', 'tokenomy',
    'tokenxpro', 'tokok', 'topbtc', 'tradebytrade', 'tradeogre',
    'tradesatoshi', 'trustdex', 'tuxexchange', 'ubit.vip', 'ucoin.pw',
    'uex', 'unocoin', 'upbit', 'vaultoro', 'vebitcoin', 'wavesplatform',
    'wazirx', 'wex.nz', 'xbtce', 'yex', 'yobit', 'zaif', 'zb', 'zbg',
]

CURRENCY_DECIMAL = {
    'usdt': 6
}

throttle = Throttle(5)


def get_accounts_urls():
    for exchange in EXCHANGES:
        yield 'https://etherscan.io/accounts/label/' + exchange


@retry(requests.exceptions.RequestException, logger=logger)
def get_account_balance(
        exchange_addr,
        contract_addr='0xdac17f958d2ee523a2206206994597c13d831ec7',
        currency='usdt'):
    delay = throttle.run()
    if delay:
        logger.debug('GET URL throttles. delay for %.2f' % (delay))
    url = f'https://api.etherscan.io/api?module=account&action=tokenbalance&' \
        f'contractaddress={contract_addr}&address={exchange_addr}&' \
        f'tag=latest&apikey={API_TOKEN}'
    res = requests.get(url, timeout=5)
    if not res or res.status_code != 200:
        logger.warning(
            f"get balance for addr: {exchange_addr} failed "
            f"with status code: {res.status_code}")
        return None
    body = res.json()
    if not body:
        raise ValueError(f'body is {body}')
    # Notice: the etherscan will return an int number for the balance result
    # and we can alter the int number to decimal number according to currency's
    # corresponding decimals, e.g .
    # {"result": 456789002233} the usdt decimals is 6, so the real balance shall
    # be 456789.002233
    decimal_num = get_decimal(str(body['result']), CURRENCY_DECIMAL.get(currency))
    return decimal_num


class ExchangeAddressSpider(scrapy.Spider):
    name = 'exchange_address_spider'
    allowed_domains = ['etherscan.io']
    start_urls = get_accounts_urls()

    def parse(self, response):
        addresses = response.xpath("//table//tr/td[1]/a/text()").extract()
        exchange = response.url.split('/')[-1]
        logger.info(f"handling exchange {exchange} data")
        now = datetime.datetime.now()
        addr_sql = """
            INSERT INTO exchange_eth_address(address, exchange, create_time)
            VALUES (%s, %s, %s)
        """
        balance_sql = """
            INSERT INTO exchange_balance(address, balance, create_time)
            VALUES (%s, %s, %s)
        """

        for addr in addresses:
            logger.info(f"insert into exchange_eth_address with values: {addr}, {exchange}")
            try:
                execute_sql(addr_sql, (addr, exchange, now))
            except MySQLdb._exceptions.IntegrityError:
                logger.warning(f"database integrityerror, address: {addr}")
                continue

        for addr in addresses:
            balance = get_account_balance(addr)
            if balance is None:
                continue
            logger.info(f"insert into exchange_balance with values: {addr}, {balance}")
            execute_sql(balance_sql, (addr, balance, now))
