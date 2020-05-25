import csv
from decimal import Decimal

from utils.db_helper import read_all_from_db
from utils.find_price import find_price

price_cache = {}
btc_price = find_price('buavg', 'BTC', 'USD', price_cache)
eth_price = find_price('buavg', 'ETH', 'USD', price_cache)

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
    group by exchange
) A  
on A.exchange=exchange_btc_balances.exchange
and A.max_create_time=exchange_btc_balances.create_time
"""

reserves_balance_dict = {}

erc20_balances = read_all_from_db(ERC20_COIN_BALANCE_SQL)
for row in erc20_balances:
    exchange = row['exchange']
    currency = row['currency']
    balance = row['balance']
    update_time = row['update_time']

    if currency == 'btc':
        continue

    exchange_balance_dict = reserves_balance_dict.get(exchange)
    if exchange_balance_dict:
        exchange_balance_dict[currency.upper() + '_reserves'] = balance
        exchange_balance_dict['update_time'] = max(exchange_balance_dict['update_time'], update_time)
    else:
        reserves_balance_dict[exchange] = {currency.upper() + '_reserves': balance, 'update_time': update_time}

btc_balances = read_all_from_db(BTC_BALANCE_SQL)
for row in btc_balances:
    exchange = row['exchange']
    balance = row['balance']
    update_time = row['update_time']

    exchange_dict = reserves_balance_dict.get(exchange)
    if exchange_dict:
        exchange_dict['BTC_reserves'] = balance
        exchange_dict['update_time'] = max(exchange_dict['update_time'], update_time)
    else:
        reserves_balance_dict[exchange] = {'BTC_reserves': balance, 'update_time': update_time}

reserves_list = []
for market_name, market_data in reserves_balance_dict.items():
    market_data['market'] = market_name.lower()
    market_data.setdefault('ETH_reserves', 0)
    market_data.setdefault('USDT_reserves', 0)
    market_data.setdefault('BTC_reserves', 0)
    total_reserves = market_data.get('ETH_reserves', 0) * eth_price + market_data.get('USDT_reserves', 0) + market_data.get('BTC_reserves', 0) * btc_price
    market_data['total_reserves_USD'] = str(total_reserves.quantize(Decimal('0.00'))).rstrip('0.')
    market_data['ETH_reserves'] = market_data['ETH_reserves'] and market_data['ETH_reserves'].quantize(Decimal('0.0000'))
    market_data['USDT_reserves'] = market_data['USDT_reserves'] and market_data['USDT_reserves'].quantize(Decimal('0.00'))
    market_data['BTC_reserves'] = market_data['BTC_reserves'] and market_data['BTC_reserves'].quantize(Decimal('0.00000000'))
    reserves_list.append(market_data)

reserves_list.sort(key=lambda d: -float(d['total_reserves_USD'] or 0))

with open('reserves.csv', 'w') as file:
    writer = csv.DictWriter(file, ['market', 'BTC_reserves', 'ETH_reserves', 'USDT_reserves', 'total_reserves_USD', 'update_time'])
    writer.writeheader()
    writer.writerows(reserves_list)
    file.write(f'\nBTC_price, {btc_price.quantize(Decimal("0.00000000"))},,,,')
    file.write(f'\nETH_price, {eth_price.quantize(Decimal("0.00000000"))},,,,')
