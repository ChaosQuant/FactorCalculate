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

from base_sync import BaseSync

sys.path.append('..')

from utillities.sync_util import SyncUtil
from datetime import datetime, date
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

import config


class SyncValuation(BaseSync):
    def __init__(self):
        self.sync_util = SyncUtil()
        super(SyncValuation, self).__init__('valuation')
        self.dir = config.RECORD_BASE_DIR + self.dest_table + '/'

    def create_dest_tables(self):
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `market_cap` decimal(19,4) DEFAULT NULL,
                    `circulating_market_cap` decimal(19,4) DEFAULT NULL,
                    `turnover_ratio` decimal(9,4) DEFAULT NULL,
                    `pb` decimal(19,4) DEFAULT NULL,
                    `pe_lfy` decimal(19,4) DEFAULT NULL,
                    `pe` decimal(19,4) DEFAULT NULL,
                    `ps_lfy` decimal(19,4) DEFAULT NULL,
                    `ps` decimal(19,4) DEFAULT NULL,
                    `pcf` decimal(19,4) DEFAULT NULL,
                    `capitalization` decimal(19,4) DEFAULT NULL,
                    `circulating_cap` decimal(19,4) DEFAULT NULL,
                    PRIMARY KEY(`id`,`symbol`,`trade_date`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
                    """.format(self.dest_table)
        self.create_table(create_sql)

    def get_valutaion(self, trade_date):
        sql = """select a.SYMBOL as code,
                        a.TRADEDATE as trade_date,
                        a.TOTMKTCAP as market_cap,
                        a.NEGOTIABLEMV as circulating_market_cap,
                        a.TURNRATE as turnover_ratio,
                        a.PETTM as pe,
                        a.PELFY as pe_lfy,
                        a.PB as pb,
                        a.PSTTM as ps,
                        a.PSLFY as ps_lfy,
                        a.PCTTM as pcf,
                        b.TOTALSHARE as capitalization,
                        b.MKTSHARE as circulating_cap,
                        c.Exchange
                        from TQ_SK_FININDIC a left join TQ_SK_DQUOTEINDIC b
                        on a.SYMBOL=b.SYMBOL and a.TRADEDATE=b.TRADEDATE
                        left join TQ_OA_STCODE c
                        on a.SECODE = c.SECODE and a.SYMBOL = c.SYMBOL
                        where a.TRADEDATE='{0}';""".format(trade_date)
        return pd.read_sql(sql, self.source)

    def do_update(self, start_date, end_date, count, order='DESC'):
        # 读取交易日
        trade_sets = self.sync_util.get_trades_ago('001002', start_date, end_date, count, order)
        trade_list = list(trade_sets['TRADEDATE'])
        for trade_date in trade_list:
            print(trade_date)
            index_sets = self.get_valutaion(trade_date)
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
        processor = SyncValuation()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    if args.update:
        processor = SyncValuation()
        processor.do_update(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncValuation()
        start_date = processor.get_start_date()
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
