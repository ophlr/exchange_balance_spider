import requests

SIGNING_SERVICE = 'http://trade-signing-service.private.local'

def replace_url(url):
    if url.startswith("https://poloniex.com"):
        return url.replace(
            "https://poloniex.com",
            "http://poloniex.private.bituniverse.org"
        )
    elif url.startswith("https://bittrex.com"):
        return url.replace(
            "https://bittrex.com",
            "http://bittrex.private.bituniverse.org"
        )
    elif url.startswith("https://data.gate.io"):
        return url.replace(
            "https://data.gate.io",
            "http://gateio.private.bituniverse.org"
        )
    elif url.startswith("https://www.okex.com"):
        return url.replace(
            "https://www.okex.com",
            "http://okex.private.bituniverse.org"
        )
    elif url.startswith("https://openapi-v2.kucoin.com"):
        return url.replace(
            "https://openapi-v2.kucoin.com",
            "http://kucoin.private.bituniverse.org"
        )
    elif url.startswith("https://api.fcoin.com"):
        return url.replace(
            "https://api.fcoin.com",
            "http://fcoin.private.bituniverse.org"
        )
    elif url.startswith("https://indodax.com"):
        return url.replace(
            "https://indodax.com",
            "http://indodax.private.bituniverse.org"
        )
    elif url.startswith("https://api.cointiger.com"):
        return url.replace(
            "https://api.cointiger.com",
            "http://cointiger.private.bituniverse.org"
        )
    elif url.startswith("https://api.bitzapi.com"):
        return url.replace(
            "https://api.bitzapi.com",
            "http://bitz.private.bituniverse.org"
        )
    elif url.startswith("https://big.one"):
        return url.replace(
            "https://big.one",
            "http://bigone.private.bituniverse.org"
        )
    elif url.startswith("https://api.bitopro.com:9500"):
        return url.replace(
            "https://api.bitopro.com:9500",
            "http://bitopro.private.bituniverse.org",
        )
    elif url.startswith("https://api.ceobi.com"):
        return url.replace(
            "https://api.ceobi.com",
            "http://ceo.private.bituniverse.org",
        )
    elif url.startswith("https://api.liquid.com"):
        return url.replace(
            "https://api.liquid.com",
            "http://liquid.private.bituniverse.org",
        )
    elif url.startswith("https://api.lbank.info"):
        return url.replace(
            "https://api.lbank.info",
            "http://lbank.private.bituniverse.org",
        )
    elif url.startswith("https://api.bithumb.com"):
        return url.replace(
            "https://api.bithumb.com",
            "http://bithumb.private.bituniverse.org",
        )
    elif url.startswith("https://api.binance.com"):
        return url.replace(
            "https://api.binance.com",
            "http://binance.private.bituniverse.org",
        )
    elif url.startswith("https://api.huobi.pro"):
        # return url
        return url.replace(
            "https://api.huobi.pro",
            "http://huobipro.private.bituniverse.org",
        )
    elif url.startswith("https://bitmax.io"):
        return url.replace(
            "https://bitmax.io",
            "http://bitmax.private.bituniverse.org",
        )
    elif url.startswith("https://www.shubaoex.com"):
        return url.replace(
            "https://www.shubaoex.com",
            "http://shubaoex.private.bituniverse.org",
        )
    return url


def get_exchange_parser(exchange):
    if exchange == 'kucoin':
        return kucoin_address_parser
    elif exchange == 'big.one':
        return bigone_address_parser
    elif exchange == 'bitmax':
        return bitmax_address_parser
    elif exchange == 'poloniex':
        return poloniex_address_parser
    elif exchange == 'gate.io':
        return gateio_address_parser
    elif exchange == 'binance':
        return binance_address_parser
    elif exchange == 'huobi.pro':
        return huobi_address_parser
    elif exchange == 'okex':
        return okex_address_parser
    elif exchange == 'bittrex':
        return bittrex_address_parser
    elif exchange == 'hitbtc':
        return hitbtc_address_parser
    elif exchange == 'coinbase':
        return coinbase_address_parser
    elif exchange == 'bithumb':
        return bithumb_address_parser
    elif exchange == 'liquid':
        return liquid_address_parser

    raise Exception('not supported yet')


def kucoin_address_parser(response, **kwargs):
    address = None
    response_data = response.json().get('data')
    if response_data:
        address = response_data.get('address')

    return address


def bigone_address_parser(response, **kwargs):
    address = None
    response_data = response.json().get('data')
    if response_data:
        if kwargs['currency'] == 'USDT':
            address = []
            for d in response_data:
                if d.get('chain') == 'Bitcoin':
                    address.append(d.get('value'))
        else:
            return [d.get('value') for d in response_data]
    return address


def bitmax_address_parser(response, **kwargs):
    address = None
    if response.status_code != 200:
        return address
    print(response.content)
    response_data = response.json().get('data')
    if isinstance(response_data, list):
        address = []
        for d in response_data:
            a = d.get('addressData').get('address')
            t = d.get('blockChain')
            if t == 'Omni':
                address.append(a)
    elif isinstance(response_data, dict):
        address = response_data.get('address')

    return address


def get_extra_info_handler(exchange):
    if exchange == 'bitmax':
        return bitmax_extra_info_handler
    elif exchange == 'coinbase':
        return coinbase_extra_info_handler
    elif exchange == 'binance':
        return binance_extra_info_handler

    return None


def bitmax_extra_info_handler(token, **kwargs):
    sign_result = requests.post(f'{SIGNING_SERVICE}/sign/get_extra_info',
                                headers={'Authorization': token})
    if sign_result.status_code != 200:
        print("sign extra_info error: {} {}".format(sign_result.status_code, sign_result.content))
        return None
    data = sign_result.json()['data']
    url = data['url']
    url = replace_url(url)

    exchange_response = requests.request(data['method'], url, data=data.get('body', None),
                                         headers=data.get('headers', None))

    if exchange_response.status_code != 200:
        print("extra_info exchange error: {} {}".format(exchange_response.status_code, exchange_response.content))
        return None

    group = exchange_response.json().get('accountGroup')
    if group is None:
        print("get group error: {} {}".format(exchange_response.status_code, exchange_response.content))
        return None

    return {'group': group}


def poloniex_address_parser(response, **kwargs):
    address = None
    if response.status_code != 200:
        return address
    currency = kwargs['currency']
    response_data = response.json()
    if response_data:
        address = response_data.get(currency)
    return address


def gateio_address_parser(response, **kwargs):
    address = None
    if response.status_code != 200:
        return address
    response_data = response.json()
    if response_data:
        address = response_data.get('addr')
    return address


def binance_address_parser(response, **kwargs):
    address = None

    try:
        response = response.json()
    except Exception as e:
        print(e)
        print(response.content)
        return address

    if 'success' in response:
        if response['success']:
            address = response.get('address')

    return address


def huobi_address_parser(response, **kwargs):
    address = None

    if response.status_code != 200:
        return address

    response_data = response.json().get('data')
    if not response_data:
        return address
    if isinstance(response_data, list):
        address = []
        if kwargs.get('currency').upper() == 'USDT':
            for data in response_data:
                if data.get('chain') == 'usdt':
                    data_address = data.get('address')
                    address.append(data_address)
        else:
            for data in response_data:
                data_address = data.get('address')
                address.append(data_address)
    else:
        address = response_data.get('address')

    return address


def okex_address_parser(response, **kwargs):
    address = None

    if response.status_code != 200:
        return address

    response_data = response.json()
    if isinstance(response_data, list):
        address = []
        if kwargs.get('currency').upper() == 'USDT':
            for data in response_data:
                if data.get('currency') == 'usdt':
                    data_address = data.get('address')
                    address.append(data_address)
        else:
            for data in response_data:
                data_address = data.get('address')
                address.append(data_address)
    else:
        address = response_data.get('address')

    return address


def bittrex_address_parser(response, **kwargs):
    address = None

    if response.status_code != 200:
        return address

    print(response.content)
    response_data = response.json().get('result')
    if not response_data:
        return address
    if isinstance(response_data, list):
        address = []
        for data in response_data:
            data_address = data.get('Address')
            address.append(data_address)
    else:
        address = response_data.get('Address')

    return address


def hitbtc_address_parser(response, **kwargs):
    address = None

    if response.status_code != 200:
        return address

    print(response.content)
    response_data = response.json()
    if not response_data:
        return address
    if isinstance(response_data, list):
        address = []
        for data in response_data:
            data_address = data.get('address')
            address.append(data_address)
    else:
        address = response_data.get('address')

    return address


def coinbase_extra_info_handler(token, **kwargs):
    sign_result = requests.post(f'{SIGNING_SERVICE}/sign/get_extra_info',
                                headers={'Authorization': token})
    if sign_result.status_code != 200:
        print("sign extra_info error: {} {}".format(sign_result.status_code, sign_result.content))
        return None
    data = sign_result.json()['data']
    url = data['url']
    url = replace_url(url)

    exchange_response = requests.request(data['method'], url, data=data.get('body', None),
                                         headers=data.get('headers', None))

    if exchange_response.status_code != 200:
        print("extra_info exchange error: {} {}".format(exchange_response.status_code, exchange_response.content))
        return None

    accounts = exchange_response.json().get('data')
    currency = kwargs['currency']
    account_id = None
    for account in accounts:
        currency_data = account.get('currency')

        if isinstance(currency_data, dict):
            currency_code = currency_data.get('code')
        else:
            currency_code = currency_data

        if currency_code == currency.upper():
            account_id = account.get('id')
            break

    if account_id is None:
        print("get account_id error: {} {}".format(exchange_response.status_code, exchange_response.content))
        return None

    return {'account_id': account_id}


def binance_extra_info_handler(token, **kwargs):
    currency = kwargs['currency']
    if currency == 'USDT':
        return {'network': 'OMNI'}
    else:
        return {}

def coinbase_address_parser(response, **kwargs):
    address = None

    if response.status_code != 200:
        return address

    response_data = response.json().get('data')
    if not response_data:
        return address

    if isinstance(response_data, list):
        address = []
        for data in response_data:
            data_address = data.get('address')
            address.append(data_address)
    else:
        address = response_data.get('address')

    return address


def bithumb_address_parser(response, **kwargs):
    address = None

    if response.status_code != 200:
        return address

    response_data = response.json().get('data')
    if not response_data:
        return address

    if isinstance(response_data, list):
        address = []
        for data in response_data:
            data_address = data.get('wallet_address')
            address.append(data_address)
    else:
        address = response_data.get('wallet_address')

    return address


def liquid_address_parser(response, **kwargs):
    address = None
    currency = kwargs['currency']

    if response.status_code != 200:
        return address

    response_data = response.json()
    if not response_data:
        return address

    if isinstance(response_data, list):
        address = []
        for data in response_data:
            if data.get('currency') == currency:
                data_address = data.get('address')
                address.append(data_address)
    elif response_data.get('currency') == currency:
        address = response_data.get('address')

    return address