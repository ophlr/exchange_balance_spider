import csv
from decimal import Decimal

import redis

from utils.db_helper import read_all_from_db
from utils.find_price import find_price

redis_client = redis.StrictRedis('portfolio.ticker.redis.private.bituniverse.org', decode_responses=True)

ERC20_COIN_BALANCE_SQL = """
select 
    exchange,
    currency,
    sum(balance) as balance,
    max(B.create_time) as update_time
from exchange_eth_address
join (
    select 
        exchange_balance.address,
        exchange_balance.currency,
        exchange_balance.balance,
        exchange_balance.create_time
    from exchange_balance
    join (
        select
            address,
            currency,
            max(create_time) as max_create_time
        from exchange_balance
        where year(create_time) = year(now()) and week(create_time) = week(now()) - %s
        group by address, currency
    ) A  
    on A.address=exchange_balance.address
    and A.currency=exchange_balance.currency
    and A.max_create_time=exchange_balance.create_time
) B using(address)
group by exchange, currency
"""

BTC_BALANCE_SQL = """
select 
    exchange_btc_balances.exchange,
    exchange_btc_balances.balance,
    exchange_btc_balances.create_time as update_time
from exchange_btc_balances
join (
    select
        exchange,
        balance,
        max(create_time) as max_create_time
    from exchange_btc_balances
    where year(create_time) = year(now()) and week(create_time) = week(now()) - %s
    group by exchange
) A  
on A.exchange=exchange_btc_balances.exchange
and A.max_create_time=exchange_btc_balances.create_time
"""

reserves_balance_dict = {}

erc20_balances = read_all_from_db(ERC20_COIN_BALANCE_SQL, [2])
for row in erc20_balances:
    exchange = row['exchange']
    currency = row['currency']
    balance = row['balance']
    update_time = row['update_time']

    if currency == 'btc':
        continue

    key = exchange
    exchange_balance_dict = reserves_balance_dict.get(key)
    if exchange_balance_dict:
        exchange_balance_dict[currency.upper() + '_from'] = balance
        exchange_balance_dict['from_time'] = max(exchange_balance_dict['from_time'], update_time)
    else:
        reserves_balance_dict[key] = {'exchange': exchange, currency.upper() + '_from': balance, 'from_time': update_time}

btc_balances = read_all_from_db(BTC_BALANCE_SQL, [2])
for row in btc_balances:
    exchange = row['exchange']
    balance = row['balance']
    update_time = row['update_time']

    key = exchange
    exchange_dict = reserves_balance_dict.get(key)
    if exchange_dict:
        exchange_dict['BTC_from'] = balance
        exchange_dict['from_time'] = max(exchange_dict['from_time'], update_time)
    else:
        reserves_balance_dict[key] = {'exchange': exchange, 'BTC_from': balance, 'from_time': update_time}

reserves_new_balance_dict = {}

erc20_balances = read_all_from_db(ERC20_COIN_BALANCE_SQL, [1])
for row in erc20_balances:
    exchange = row['exchange']
    currency = row['currency']
    balance = row['balance']
    update_time = row['update_time']

    if currency == 'btc':
        continue

    key = exchange
    exchange_balance_dict = reserves_new_balance_dict.get(key)
    if exchange_balance_dict:
        exchange_balance_dict[currency.upper() + '_to'] = balance
        exchange_balance_dict['to_time'] = max(exchange_balance_dict['to_time'], update_time)
    else:
        reserves_new_balance_dict[key] = {'exchange': exchange, currency.upper() + '_to': balance, 'to_time': update_time}

btc_balances = read_all_from_db(BTC_BALANCE_SQL, [1])
for row in btc_balances:
    exchange = row['exchange']
    balance = row['balance']
    update_time = row['update_time']

    key = exchange
    exchange_dict = reserves_new_balance_dict.get(key)
    if exchange_dict:
        exchange_dict['BTC_to'] = balance
        exchange_dict['to_time'] = max(exchange_dict['to_time'], update_time)
    else:
        reserves_new_balance_dict[key] = {'exchange': exchange, 'BTC_to': balance, 'to_time': update_time}

reserves_list = []
for market_name, market_data in reserves_balance_dict.items():
    market_data['market'] = market_data.pop('exchange').lower()

    market_data.setdefault('ETH_from', 0)
    market_data.setdefault('USDT_from', 0)
    market_data.setdefault('BTC_from', 0)
    market_data['ETH_from'] = market_data['ETH_from'] and market_data['ETH_from'].quantize(Decimal('0.0000'))
    market_data['USDT_from'] = market_data['USDT_from'] and market_data['USDT_from'].quantize(Decimal('0.00'))
    market_data['BTC_from'] = market_data['BTC_from'] and market_data['BTC_from'].quantize(Decimal('0.00000000'))

    new_market_data = reserves_new_balance_dict.get(market_data['market'], {})

    new_market_data.setdefault('ETH_to', 0)
    new_market_data.setdefault('USDT_to', 0)
    new_market_data.setdefault('BTC_to', 0)
    market_data['ETH_to'] = new_market_data['ETH_to'] and new_market_data['ETH_to'].quantize(Decimal('0.0000'))
    market_data['USDT_to'] = new_market_data['USDT_to'] and new_market_data['USDT_to'].quantize(Decimal('0.00'))
    market_data['BTC_to'] = new_market_data['BTC_to'] and new_market_data['BTC_to'].quantize(Decimal('0.00000000'))

    market_data['ETH_change'] = market_data['ETH_to'] - market_data['ETH_from']
    market_data['USDT_change'] = market_data['USDT_to'] - market_data['USDT_from']
    market_data['BTC_change'] = market_data['BTC_to'] - market_data['BTC_from']

    market_data['to_time'] = new_market_data.get('to_time')

    reserves_list.append(market_data)


def get_rank(data):
    market = data['market']
    key = "coincap|||market_rank|||%s" % market
    rank_str = redis_client.get(key) or '1000'
    return rank_str.rjust(5, '0') + market


reserves_list.sort(key=get_rank)

with open('reserves_weekly_change.csv', 'w') as file:
    writer = csv.DictWriter(file, ['market',
                                   'BTC_from', 'BTC_to', 'BTC_change',
                                   'ETH_from', 'ETH_to', 'ETH_change',
                                   'USDT_from', 'USDT_to', 'USDT_change',
                                   'from_time', 'to_time'])
    writer.writeheader()
    writer.writerows(reserves_list)
