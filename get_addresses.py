import argparse
import requests

from utils.db_helper import read_all_from_db
from utils.deposit_address_parser import replace_url, get_exchange_parser, get_extra_info_handler

parser = argparse.ArgumentParser(description="import btc csv file")
parser.add_argument('--exchange', '-e', action='store', help='exchange', type=str, required=True)
parser.add_argument('--currency', '-c', action='store', help='currency', type=str, required=True)

args = parser.parse_args()
exchange = args.exchange
currency = args.currency

TRADE_TOKEN_SERVICE_HOST = 'http://trade-token-service.private.local'
SIGNING_SERVICE = 'http://trade-signing-service.private.local'


def main():
    addresses = set()

    key_list = read_all_from_db('select user_id, key_id from KeyManager_userkeyinfo where exchange = %s and enabled = TRUE order by id desc', [exchange])

    total_count = len(key_list)
    print("key number: {}".format(total_count))

    index = 0
    for key in key_list:
        user_id = key['user_id']
        key_id = key['key_id']
        token_body = {
            "user_id": user_id,
            "key_id": key_id,
            "exchange": exchange,
        }
        token_response = requests.post(f'{TRADE_TOKEN_SERVICE_HOST}/sign_address_token', json=token_body)
        if token_response.status_code != 200:
            print("sign token error: {} {}, {}".format(token_response.status_code, token_response.content, token_body))
            continue

        token_data = token_response.json()['data']

        extra_dict = {}
        extra_info_handler = get_extra_info_handler(exchange)

        if extra_info_handler:
            extra_dict = extra_info_handler(token_data['token'], currency=currency)

        json_body = {'currency': currency}

        if extra_dict:
            json_body.update(extra_dict)

        sign_result = requests.post(f'{SIGNING_SERVICE}/sign/get_deposit_address', json=json_body, headers={'Authorization': token_data['token']})
        if sign_result.status_code != 200:
            print("sign response error: {} {}".format(sign_result.status_code, sign_result.content))
            continue
        data = sign_result.json()['data']
        # print(data)
        url = data['url']
        url = replace_url(url)

        try:
            exchange_response = requests.request(data['method'], url, data=data.get('body', None), headers=data.get('headers', None))
        except Exception as e:
            print(f'request error: {e}, {data}')
            continue

        address = get_exchange_parser(exchange)(exchange_response, currency=currency)

        if address:
            if isinstance(address, list):
                for a in address:
                    addresses.add(a)
            else:
                addresses.add(address)
        else:
            print(str(exchange_response.status_code) + ': ' + exchange_response.content.decode())

        index += 1
        if index % 10 == 0:
            print("progress: {}/{}, address count: {}".format(index, total_count, len(addresses)))

    print(addresses)


main()
