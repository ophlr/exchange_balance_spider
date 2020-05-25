#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
main.py
~~~~~~~~~~~~
.

:copyright: (c) 2019 by Ziqian Liu.

"""
from scrapy import cmdline
from utils.log_util import setup_logging


setup_logging()
cmdline.execute('scrapy crawl exchange_address_spider'.split())