#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
merge_balance.py
~~~~~~~~~~~~
merge balances of exchanges.
there are kinds of balances for each exchange, e.g. BTC balances, USDT balances.
we need to merge those balances into one table.

:copyright: (c) 2019 by Ziqian Liu.

"""
from utils.db_helper import read_all_from_db
from utils.common_util import get_updated_date


def get_eth_usdt_balance():
    # get recent record date
    updated_date = get_updated_date('exchange_balance')

    sql = """
        SELECT 
            exchange, currency, SUM(balance) as sum_balance
        FROM
            exchange_balance AS eb JOIN exchange_eth_address AS eea
            ON eea.address = eb.address
        WHERE 
            eb.create_time > %s
        GROUP BY 
            exchange, currency
    """

    records = read_all_from_db(sql, (updated_date,))
    return records


def get_btc_balance():
    # get recent record date
    updated_date = get_updated_date('exchange_btc_balances')

    sql = """
    SELECT 
        exchange, balance
    FROM 
        exchange_btc_balances
    WHERE
        create_time > %s
    """

    records = read_all_from_db(sql, (updated_date,))
    return records


def main():
    # get eth/usdt balance of exchanges
    eth_usdt_balance = get_eth_usdt_balance()

    # get btc balance of exchanges
    btc_balance = get_btc_balance()

    # merge balances
    merged_balance = dict()
    for rec in eth_usdt_balance:
        exchange = rec.pop('exchange')
        merged_balance[exchange] = {rec['currency']: rec['sum_balance']}

    for rec in btc_balance:
        exchange = rec['exchange']
        balance = rec['balance']
        if exchange in merged_balance:
            merged_balance[exchange].update({'btc': balance})
        else:
            merged_balance[exchange] = {'btc': balance}

    with open('merged_balance.csv', 'w') as f:
        f.write('exchange, btc, usdt, eth\n')
        for exchange, balances in merged_balance.items():
            btc_balance = str(balances.get('btc', 0))
            eth_balance = str(balances.get('eth', 0))
            usdt_balance = str(balances.get('usdt', 0))
            f.write(
                f"{exchange}, {btc_balance}, "
                f"{usdt_balance}, {eth_balance}\n")


if __name__ == '__main__':
    main()
