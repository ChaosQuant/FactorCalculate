#!/usr/bin/env python
# coding=utf-8

import pdb
import sys
import os
import sqlalchemy as sa
import pandas as pd
import numpy as np
import collections
import argparse
from datetime import datetime, date
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from base_sync import BaseSync

sys.path.append('..')
import config
from utillities.sync_util import SyncUtil


class SyncIndexDailyPrice(BaseSync):
    def __init__(self):
        self.sync_util = SyncUtil()
        super(SyncIndexDailyPrice, self).__init__('index_daily_price')
        self.dir = config.RECORD_BASE_DIR + self.dest_table + '/'

    def create_dest_tables(self):
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(32) NOT NULL,
                    `trade_date` date NOT NULL,
                    `name` varchar(50) CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'  NOT NULL,
                    `pre_close` decimal(15,6) DEFAULT NULL,
                    `open` decimal(15,6) DEFAULT NULL,
                    `close` decimal(15,6) DEFAULT NULL,
                    `high` decimal(15,6) DEFAULT NULL,
                    `low` decimal(15,6) DEFAULT NULL,
                    `volume` decimal(20,2) DEFAULT NULL,
                    `money` decimal(18,3) DEFAULT NULL,
                    `deals` decimal(10,0) DEFAULT NULL,
                    `change` decimal(9,4) DEFAULT NULL,
                    `change_pct` decimal(8,4) DEFAULT NULL,
                    `tot_mkt_cap` decimal(18,4) DEFAULT NULL,
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
                    """.format(self.dest_table)
        self.create_table(create_sql)

    def get_index_sets(self, trade_date):
        sql = """SELECT TRADEDATE as trade_date,
                    i.exchange as Exchange,
                    s.symbol as code,
                    INDEXNAME as name,
                    LCLOSE as pre_close,
                    TOPEN as 'open',
                    TCLOSE as 'close',
                    THIGH as high,
                    TLOW as low,
                    VOL as volume,
                    AMOUNT as money,
                    DEALS as deals,
                    CHANGE as change,
                    PCHG as change_pct,
                    TOTMKTCAP as tot_mkt_cap
                    from QADB.dbo.TQ_QT_INDEX i
                    left join TQ_OA_STCODE s on i.SECODE = s.secode
               where (i.exchange = '001002' or i.exchange = '001003') and i.ISVALID = 1 and s.ISVALID = 1 and TRADEDATE = '{0}';""".format(
            trade_date)
        return pd.read_sql(sql, self.source)

    def do_update(self, start_date, end_date, count, order='DESC'):
        # 读取交易日
        # 路径 index/
        trade_sets = self.sync_util.get_trades_ago('001002', start_date, end_date, count, order)
        trade_list = list(trade_sets['TRADEDATE'])
        for trade_date in trade_list:
            print(trade_date)
            index_sets = self.get_index_sets(trade_date)
            if index_sets.empty:
                continue
            try:
                index_sets['symbol'] = np.where(index_sets['Exchange'] == '001002',
                                                index_sets['code'] + '.XSHG',
                                                index_sets['code'] + '.XSHE')
                index_sets['id'] = index_sets['symbol'] + str(trade_date)
                index_sets.drop(['Exchange', 'code'], axis=1, inplace=True)

                # 本地保存
                if not os.path.exists(self.dir):
                    os.makedirs(self.dir)
                file_name = self.dir + str(trade_date) + '.csv'
                if os.path.exists(str(file_name)):
                    os.remove(str(file_name))
                index_sets.to_csv(file_name, encoding='UTF-8')

                # 数据库保存
                try:
                    self.delete_trade_data(trade_date)
                    index_sets.to_sql(name=self.dest_table, con=self.destination, if_exists='append', index=False)
                except Exception as sql_err:
                    print(sql_err.orig.msg)
                    self.insert_or_update(index_sets)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=2)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = SyncIndexDailyPrice()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    if args.update:
        processor = SyncIndexDailyPrice()
        processor.do_update(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncIndexDailyPrice()
        start_date = processor.get_start_date()
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
