import simplejson as json
import logging
from decimal import Decimal

import redis

ticker_redis = redis.StrictRedis(host='portfolio.ticker.redis.private.bituniverse.org', port='6379', db='0')


def _is_stable_currency(exchange, currency):
    if currency == 'USDT' or currency == 'USD':
        return True
    if exchange == 'huobi.pro':
        if currency == 'HUSD' or currency == 'HBPOINT':
            return True
    if exchange == 'kucoin':
        if currency == 'DAI' or currency == 'PAX' or currency == 'TUSD':
            return True
    if exchange == 'okex' and currency == 'OKDK':
        return True
    return False


def find_price_from_ticker(exchange, base, quote):
    if exchange != 'buavg':
        redis_query_key = "balance|||{}|||{}|||{}".format(exchange, base, quote)

        redis_price_key = ticker_redis.get(redis_query_key)
        if not redis_price_key:
            logging.info("cannot get price key from redis: {}".format(redis_query_key))
            return None
            # raise RuntimeError("cannot get price from redis: {}".format(redis_query_key))
    else:
        redis_price_key = "mbq|||{}|||{}|||{}".format(exchange, base, quote)

    redis_price_data = ticker_redis.get(redis_price_key)
    if not redis_price_data:
        logging.info("cannot get price from redis: {}".format(redis_price_key))
        return None
        # raise RuntimeError("cannot get price from redis: {}, {}".format(redis_query_key, redis_price_key))

    redis_price_data_dict = json.loads(redis_price_data)
    if quote == 'USD' and exchange != 'buavg':
        price = Decimal(redis_price_data_dict['fiat'])
    else:
        price = Decimal(redis_price_data_dict['price'])
    return price


def find_price(exchange, base, quote, price_cache=None):
    if base == quote:
        return Decimal(1)

    if _is_stable_currency(exchange, quote):
        quote = 'USD'

    if quote == 'USD' and _is_stable_currency(exchange, base):
        return Decimal(1)

    price_key = "{}_{}_{}".format(exchange, base, quote)
    cached_price = price_cache and price_cache.get(price_key)
    if cached_price:
        return cached_price
    price = find_price_from_ticker(exchange, base, quote)
    if price is not None:
        if price_cache is not None:
            price_cache[price_key] = price
        return price

    if quote == 'USD':
        return None

    base_usd_price = find_price(exchange, base, 'USD', price_cache)
    if not base_usd_price:
        logging.error("cannot find usd price: {}-{}".format(exchange, base))
        return None
    qoute_usd_price = find_price(exchange, quote, 'USD', price_cache)
    if not qoute_usd_price:
        logging.error("cannot find usd price: {}-{}".format(exchange, quote))
        return None

    return base_usd_price / qoute_usd_price

