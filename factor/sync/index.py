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

from base_sync import BaseSync

sys.path.append('..')
from datetime import datetime, date
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

import config

from utillities.sync_util import SyncUtil


class SyncIndex(BaseSync):
    def __init__(self):
        self.sync_util = SyncUtil()
        super(SyncIndex, self).__init__('index')
        self.source = sa.create_engine("mssql+pymssql://read:read@192.168.100.87:1433/FCDB")
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
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8
                    PARTITION BY RANGE (to_days(trade_date))
                    (PARTITION p0 VALUES LESS THAN (TO_DAYS('20000101')) ENGINE = InnoDB);
                    """.format(self.dest_table)
        self.create_table(create_sql)
        self.create_index()
        self.build_history_partion(2000)

    def create_index(self):
        session = self.dest_session()
        indexs = [
            'CREATE INDEX index_trade_date_isymbol_index ON `index` (trade_date, isymbol);',
            'CREATE INDEX index_trade_date_symbol_index ON `index` (trade_date, symbol);'
        ]
        for sql in indexs:
            session.execute(sql)
        session.commit()
        session.close()

    def build_history_partion(self, start_year):
        print('正在生成', start_year, '年之后的历史分区表')
        current_year = datetime.now().year
        current_month = datetime.now().month
        session = self.dest_session()
        for i in range(start_year, current_year):
            print(i)
            for j in range(1, 13):
                if j < 10:
                    j = '0' + str(j)
                session.execute(
                    '''call SP_TABLE_PARTITION_AUTO('index','{0}','par_index');'''
                        .format(str(i) + str(j))
                )
        for j in range(1, current_month):
            if j < 10:
                j = '0' + str(j)
            session.execute(
                '''call SP_TABLE_PARTITION_AUTO('index','{0}','par_index');'''
                    .format(str(current_year) + str(j))
            )

    def get_index_sets(self, trade_date):
        sql = """SELECT Isymbol as icode, Iexchange as iexchange, Iname as iname, Tdate as trade_date,
               Symbol as code ,Exchange , Sname as sname, Weighing as weighing from FCDB.dbo.issweight
               where Isymbol in ('000300','000906','000985','399005','399006','000852','000905','399102','000016') 
               and Tdate = '{0}';""".format(trade_date)
        return pd.read_sql(sql, self.source)

    def do_update(self, start_date, end_date, count, order='DESC'):
        # 读取交易日
        trade_sets = self.sync_util.get_trades_ago('001002', start_date, end_date, count, order)
        trade_list = list(trade_sets['TRADEDATE'])
        session = self.dest_session()
        for trade_date in trade_list:
            print(trade_date)
            index_sets = self.get_index_sets(trade_date)
            if index_sets.empty:
                continue
            try:
                index_sets['symbol'] = np.where(index_sets['Exchange'] == 'CNSESH',
                                                index_sets['code'] + '.XSHG',
                                                index_sets['code'] + '.XSHE')
                index_sets['isymbol'] = np.where(index_sets['iexchange'] == 'CNSESH',
                                                 index_sets['icode'] + '.XSHG',
                                                 index_sets['icode'] + '.XSHE')
                index_sets['id'] = index_sets['symbol'] + str(trade_date) + index_sets['iexchange'] + index_sets[
                    'isymbol']
                index_sets.drop(['iexchange', 'Exchange', 'code', 'icode'], axis=1, inplace=True)

                # 本地保存
                if not os.path.exists(self.dir):
                    os.makedirs(self.dir)
                file_name = self.dir + str(trade_date) + '.csv'
                if os.path.exists(str(file_name)):
                    os.remove(str(file_name))
                index_sets.to_csv(file_name, encoding='UTF-8')

                # 数据库保存
                session.execute('''call SP_TABLE_PARTITION_AUTO('index','{0}','par_index');'''.format(trade_date))
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
        processor = SyncIndex()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    if args.update:
        processor = SyncIndex()
        processor.do_update(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncIndex()
        start_date = processor.get_start_date()
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
