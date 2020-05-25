#! /bin/bash
source /home/ubuntu/.virtualenvs/eth-crawler/bin/activate
cd /home/ubuntu/code/eth_balance_crawler
python hydax_usdt_balance_crawler.py
python renrenbit_balance_crawler.py
