#!/usr/bin/env python
# coding=utf-8

import pdb
import os
import sys
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
from utillities.sync_util import SyncUtil
import config


class SyncIndustry(BaseSync):
    def __init__(self):
        self.sync_util = SyncUtil()
        super(SyncIndustry, self).__init__('sw_industry')
        self.source = sa.create_engine("mssql+pymssql://HF_read:read@192.168.100.165:1433/FCDB")
        self.dir = config.RECORD_BASE_DIR + self.dest_table + '/'

    def create_dest_tables(self):
        create_sql = """create table `{0}`(
                    `id` varchar(128) NOT NULL,
                    `isymbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `iname` varchar(128) CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'  NOT NULL,
                    `symbol` varchar(32) NOT NULL,
                    `sname` varchar(128) CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'  NOT NULL,
                    `weighing` decimal(8,2) DEFAULT NULL,
                    PRIMARY KEY(`id`,`trade_date`,`isymbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
                    """.format(self.dest_table)
        self.create_table(create_sql)

    def get_sw_industry(self):
        sql_pe = """(SELECT Symbol from FCDB.dbo.iprofile where
                Iprofile7 ='申万一级行业指数' or Iprofile7 ='申万二级行业指数'
                or Iprofile7 ='申万三级行业指数')"""
        return pd.read_sql(sql_pe, self.source)

    def get_index_sets(self, trade_date, industry_sets):
        sql = """select Isymbol as isymbol,Tdate as trade_date,
                Iname as iname, Symbol as code,Exchange,
                Sname as sname, Weighing as weighing from FCDB.dbo.issweight where
                Isymbol in {1} and  Tdate = '{0}';""".format(trade_date, industry_sets)
        sql = sql.replace('[', '(')
        sql = sql.replace(']', ')')
        return pd.read_sql(sql, self.source)

    def do_update(self, start_date, end_date, count, order='DESC'):
        # 读取交易日
        trade_sets = self.sync_util.get_trades_ago('001002', start_date, end_date, count, order)
        sw_industry = self.get_sw_industry()
        trade_list = list(trade_sets['TRADEDATE'])
        for trade_date in trade_list:
            print(trade_date)
            index_sets = self.get_index_sets(trade_date, list(sw_industry['Symbol'].astype('str')))
            if index_sets.empty:
                continue
            try:
                index_sets['symbol'] = np.where(index_sets['Exchange'] == 'CNSESH',
                                                index_sets['code'] + '.XSHG',
                                                index_sets['code'] + '.XSHE')
                index_sets['id'] = index_sets['symbol'] + str(trade_date) + index_sets['isymbol']
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
        processor = SyncIndustry()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    if args.update:
        processor = SyncIndustry()
        processor.do_update(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncIndustry()
        start_date = processor.get_start_date()
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
