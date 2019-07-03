#!/usr/bin/env python
# coding=utf-8
import os
import sys
import pdb
import argparse
from datetime import datetime

sys.path.append('..')
import config
from utillities.sync_util import SyncUtil


class SyncTradeDate(object):
    def __init__(self):
        self._unit = SyncUtil()
        self._dir = config.RECORD_BASE_DIR + 'trade_date/'

    def do_update(self, start_date, end_date, count):
        trade_sets = self._unit.get_trades_ago('001002', start_date, end_date, count)
        trade_sets.rename(columns={'TRADEDATE': 'trade_date'}, inplace=True)
        # 本地保存
        file_name = self._dir + 'trade_date.csv'
        if os.path.exists(str(file_name)):
            os.remove(str(file_name))
        trade_sets.to_csv(file_name, encoding='utf-8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=-1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    args = parser.parse_args()
    if args.rebuild:
        if args.end_date == 0:
            end_date = int(str(datetime.now().date()).replace('-', ''))
        else:
            end_date = args.end_date
        processor = SyncTradeDate()
        # processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
