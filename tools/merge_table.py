#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
merge_table.py
~~~~~~~~~~~~
.

:copyright: (c) 2019 by Ziqian Liu.

"""


def main():
    usdt_dict = {}
    eth_dict = {}
    with open('usdt.csv') as usdt:
        for line in usdt:
            exchange, currency, balance = line.split('\t')
            if currency == 'usdt':
                usdt_dict[exchange] = balance.strip('\n').rstrip('0.')
            if currency == 'eth':
                eth_dict[exchange] = balance.strip('\n').rstrip('0.')


    btc_dict = {}
    with open('btc.csv') as btc:
        for line in btc:
            date, exchange, balance = line.split(',')
            exchange = exchange.strip('"').rstrip('0.')
            btc_dict[exchange] = balance.strip('\n')

    with open('merged.csv', 'w') as merge:
        merge.write('exchange, btc_balance, eth_balance, usdt_balance\n')
        for exchange, btc_balance in btc_dict.items():
            usdt_balance = usdt_dict.pop(exchange, 0)
            eth_balance = eth_dict.pop(exchange, 0)
            merge.write(f'{exchange}, {btc_balance}, {eth_balance}, {usdt_balance}\n')

        for exchange, usdt_balance in usdt_dict.items():
            btc_balance = 0
            eth_balance = eth_dict.pop(exchange, 0)
            merge.write(f'{exchange}, {btc_balance}, {eth_balance}, {usdt_balance}\n')


if __name__ == '__main__':
    main()
