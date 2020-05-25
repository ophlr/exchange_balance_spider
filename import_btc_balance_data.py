import argparse
import csv

from utils.db_helper import execute_sql

parser = argparse.ArgumentParser(description="import btc csv file")
parser.add_argument('--file', '-f', action='store', help='file path', type=str, required=True)
args = parser.parse_args()

with open(args.file, 'r') as file:
    fieldnames = ['_st_day', 'name', 'balance']
    reader = csv.DictReader(file, fieldnames=fieldnames, delimiter=',')
    for row in reader:
        execute_sql('insert into exchange_btc_balances (exchange, balance, create_time) values (%s, %s, %s)', (row['name'], row['balance'], row['_st_day'] + ' 23:59:59'))
