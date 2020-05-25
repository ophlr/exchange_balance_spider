import sys

from utils.db_helper import execute_sql

exchange = sys.argv[1]
file_path = sys.argv[2]

with open(file_path) as file:
    for line in file:
        line_str = line.strip()
        components = line_str.split(',')
        address = components[0]
        tag = components[1]
        result = execute_sql('replace into exchange_eth_address (address, exchange, tag) values (%s, %s, %s)', [address, exchange, tag])
        print(f'row effect: {result}')
