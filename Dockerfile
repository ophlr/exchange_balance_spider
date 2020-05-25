FROM 651538853910.dkr.ecr.us-west-2.amazonaws.com/base-python:base_eth_balance_crawler
COPY . /deploy/app/eth_balance_crawler
WORKDIR /deploy/app/eth_balance_crawler
CMD python exchange_currency_info.py