#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
db_helper.py
~~~~~~~~~~~~
db related operations

:copyright: (c) 2019 by Ziqian Liu.
"""
import MySQLdb
import logging
import MySQLdb.cursors as cursors
import threading
from DBUtils.PersistentDB import PersistentDB
from configs.db_config import db_config


lg = logging.getLogger()


db = PersistentDB(
    creator=MySQLdb,
    threadlocal=threading.local,
    cursorclass =cursors.DictCursor,
    **db_config
)


def read_one_from_db(sql, params=[]):
    conn = db.connection()
    try:
        with conn.cursor(cursorclass=cursors.DictCursor) as cursor:
            cursor.execute(sql, params)
            # lg.info("query row: {}, {}, {}".format(res, sql, param))
            result = cursor.fetchone()
    except Exception as e:
        # lg.exception(e)
        raise
    finally:
        conn.commit()
        conn.close()
    return result


def read_all_from_db(sql, params=[]):
    conn = db.connection()
    try:
        with conn.cursor(cursorclass=cursors.DictCursor) as cursor:
            cursor.execute(sql, params)
            # lg.info("query row: {}, {}, {}".format(res, sql, param))
            result = cursor.fetchall()
    except Exception as e:
        # lg.exception(e)
        raise
    finally:
        conn.commit()
        conn.close()
    return result


def execute_sql(sql, params = []):
    conn = db.connection()
    try:
        with conn.cursor(cursorclass=cursors.DictCursor) as cursor:
            nrow = cursor.execute(sql, params)
            conn.commit()
            lg.info('rows effected: {}'.format(nrow))

            if nrow == 0:
                # lg.error(
                #     'update row number is 0. '
                #     'sql: {} params: {}'.format(sql, params))
                return False

            return True
    except Exception as e:
        # lg.exception(e)
        raise
    finally:
        conn.close()


def execute_in_transaction(runnable):
    conn = db.connection()
    conn.begin()
    try:
        with conn.cursor(cursorclass=cursors.DictCursor) as cursor:
            runnable(cursor)
            conn.commit()

            return True
    except Exception as e:
        conn.rollback()
        lg.exception(e)
        raise
    finally:
        conn.close()
