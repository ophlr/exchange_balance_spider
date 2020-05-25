import argparse
import csv
import datetime
from decimal import Decimal

from utils.db_helper import execute_sql, read_all_from_db


parser = argparse.ArgumentParser(description="get history data")
parser.add_argument('--date', '-d', action='store', help='from date', type=str, required=True)
args = parser.parse_args()


sql_0 = """
drop TABLE erc_daily_balances;
"""

sql_1 = """
CREATE TABLE erc_daily_balances (
    exchange VARCHAR(50) NOT NULL,
    currency VARCHAR(50) NOT NULL,
    balance DECIMAL(30,10) NOT NULL,
    date date not null
);
"""

sql_2 = """
insert into erc_daily_balances (exchange, currency, balance, date) 
select 
    exchange_eth_address.exchange, currency, sum(B.balance) as balance, B.date as date
from exchange_eth_address
left join (
    select 
        exchange_balance.address, exchange_balance.currency, balance, date(create_time) as date 
    from exchange_balance 
    join (
        select 
            address, currency, max(create_time) as max_time 
        from exchange_balance 
        group by address, currency, date(create_time)
    ) A on 
        exchange_balance.address = A.address
        and exchange_balance.currency = A.currency
        and exchange_balance.create_time = A.max_time
) B using (address)
group by exchange_eth_address.exchange, currency, B.date
"""

sql_3 = """
insert into erc_daily_balances (exchange, currency, balance, date) 
select
    exchange_btc_balances.exchange, 'btc' as currency, balance, date(create_time) as date
from exchange_btc_balances
join (
    select 
        exchange, max(create_time) as max_time
    from exchange_btc_balances
    group by exchange, date(create_time)
) A
on exchange_btc_balances.exchange = A.exchange and create_time = A.max_time
"""

sql_4 = """
select * from (
    select
        A.exchange, t_btc.balance as btc_balance, t_eth.balance as eth_balance, t_usdt.balance as usdt_balance, t_eth.date as date
    from (
        select distinct exchange from erc_daily_balances
    ) as A
    left join (
        select exchange, balance, date
        from erc_daily_balances
        where currency = 'eth'
    ) as t_eth using(exchange)
    left join (
        select exchange, balance, date
        from erc_daily_balances
        where currency = 'usdt'
    ) as t_usdt on 
        t_usdt.exchange = A.exchange 
        and t_usdt.date = t_eth.date
    left join (
        select exchange, balance, date
        from erc_daily_balances
        where currency = 'btc' and date >= %s
    ) as t_btc on 
        t_btc.exchange = A.exchange 
        and t_btc.date = t_eth.date
    union distinct
    select
        A.exchange, t_btc.balance as btc_balance, t_eth.balance as eth_balance, t_usdt.balance as usdt_balance, t_btc.date as date
    from (
        select distinct exchange from erc_daily_balances
    ) as A
    right join (
        select exchange, balance, date
        from erc_daily_balances
        where currency = 'btc' and date >= %s
    ) as t_btc on 
        t_btc.exchange = A.exchange 
    left join (
        select exchange, balance, date
        from erc_daily_balances
        where currency = 'eth'
    ) as t_eth on
        t_eth.exchange = A.exchange
        and t_eth.date = t_btc.date
    left join (
        select exchange, balance, date
        from erc_daily_balances
        where currency = 'usdt'
    ) as t_usdt on 
        t_usdt.exchange = A.exchange 
        and t_usdt.date = t_btc.date
) as T
order by exchange, date
"""

execute_sql(sql_0)
execute_sql(sql_1)
execute_sql(sql_2)
execute_sql(sql_3)
data_list = read_all_from_db(sql_4, [args.date, args.date])

QUERY_HISTORY_PRICE = """
select 
    A.base, usd_price, A.format_date
from 
    bu_history_price_average join (
    select 
        max(id) as id, base, 
        date_format(from_unixtime(timestamp), '%%Y-%%m-%%d') as format_date 
    from 
        bu_history_price_average 
    where 
        timestamp > %s and (base='btc' or base='eth') 
    group by 
        base, format_date
    ) A using(id)
"""


history_price_cache = {}


def get_history_price(days):
    """get last x days price for btc and eth"""
    today = datetime.datetime.now()
    start_date = today - datetime.timedelta(days)
    timestamp = int(start_date.timestamp())
    history_pricies = read_all_from_db(QUERY_HISTORY_PRICE, (timestamp,))
    for price_data in history_pricies:
        base = price_data['base']
        usd_price = price_data['usd_price']
        format_date = price_data['format_date']
        key = '%s_%s' % (format_date, base.lower())
        history_price_cache[key] = usd_price


today_date = datetime.date.today()
from_date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
days = (today_date - from_date).days
get_history_price(days)


for data in data_list:
    date = data['date']
    if not date:
        continue
    data['eth_price'] = Decimal(history_price_cache.get("%s_%s" % (date, 'eth'))).quantize(Decimal('0.00'))
    data['btc_price'] = Decimal(history_price_cache.get("%s_%s" % (date, 'btc'))).quantize(Decimal('0.00'))
    data['total_usd'] = Decimal(data.get('btc_balance') or 0) * data['btc_price'] + Decimal(data.get('eth_balance') or 0) * data['eth_price'] + Decimal(data.get('usdt_balance') or 0)
    data['btc_balance'] = data['btc_balance'] and Decimal(data['btc_balance']).quantize(Decimal('0.00000000')) or None
    data['eth_balance'] = data['eth_balance'] and Decimal(data['eth_balance']).quantize(Decimal('0.0000')) or None
    data['usdt_balance'] = data['usdt_balance'] and Decimal(data['usdt_balance']).quantize(Decimal('0.00')) or None
    data['total_usd'] = data['total_usd'] and Decimal(data['total_usd']).quantize(Decimal('0.00')) or None


with open('history_reserves.csv', 'w') as file:
    writer = csv.DictWriter(file, ['date', 'exchange', 'btc_balance', 'eth_balance', 'usdt_balance', 'total_usd', 'btc_price', 'eth_price'])
    writer.writeheader()
    writer.writerows(data_list)
