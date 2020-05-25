#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
log_util.py
~~~~~~~~~~~~
to config system logger.

:copyright: (c) 2019 by Ziqian Liu.

"""
import os
import json
import logging.config


def setup_logging():
    """Setup logging configuration"""
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(cur_dir, "../configs")
    config_file = os.path.join(config_dir, "log_config.json")

    if os.path.exists(config_file):
        with open(config_file, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    setup_logging()
