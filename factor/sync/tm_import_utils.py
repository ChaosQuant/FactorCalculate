#!/usr/bin/env python
# coding=utf-8
import datetime
import os
import sys
import pdb
import sqlalchemy as sa
import pandas as pd
import numpy as np
from sqlalchemy.orm import sessionmaker

sys.path.append('..')
import config


class TmImportUtils(object):
    def __init__(self, source, destination, source_table, dest_table):
        # 源数据库
        self.source = source
        # 目标数据库
        self.destination = destination
        # 目标数据库Session
        self.dest_session = sessionmaker(bind=self.destination, autocommit=False, autoflush=True)
        self.source_table = source_table
        self.dest_table = dest_table
        self._dir = config.RECORD_BASE_DIR
        self._secondary_func = None

    def create_dest_tables(self, create_sql):
        drop_sql = """drop table if exists `{0}`;""".format(self.dest_table)
        session = self.dest_session()
        session.execute(drop_sql)
        session.execute(create_sql)
        session.commit()
        session.close()

    def get_max_tm_source(self):
        sql = 'select max(cast(tmstamp as bigint))as tm from ' + self.source_table
        trades_sets = pd.read_sql(sql, self.source)
        tm = 0
        if not trades_sets.empty:
            tm = trades_sets['tm'][0]
        return tm

    def get_min_tm_source(self):
        sql = 'select min(cast(tmstamp as bigint))as tm from ' + self.source_table
        trades_sets = pd.read_sql(sql, self.source)
        tm = 0
        if not trades_sets.empty:
            tm = trades_sets['tm'][0]
        return tm

    def get_max_tm_log(self):
        sql = """select max_tag from update_log where task_name='{0}'""".format(self.dest_table)
        trades_sets = pd.read_sql(sql, self.destination)
        tm = 0
        if not trades_sets.empty:
            tm = trades_sets['max_tag'][0]
        return tm

    def update_update_log(self, tm):
        session = self.dest_session()
        sql = """insert into  update_log (task_name,max_tag) values ('{0}',{1}) 
                ON DUPLICATE KEY UPDATE task_name='{0}',max_tag={1}""".format(self.dest_table, tm)
        sql = sql.replace('\n', '')
        session.execute(sql)
        session.commit()
        session.close()
        print('更新', self.dest_table, '的max_tag为', tm)

    def create_report_date(self, min_year, max_year):
        report_date_list = []
        start_date = min_year - 1
        while start_date < max_year:
            report_date_list.append(start_date * 10000 + 331)
            report_date_list.append(start_date * 10000 + 630)
            report_date_list.append(start_date * 10000 + 930)
            report_date_list.append(start_date * 10000 + 1231)
            start_date += 1
        report_date_list.sort()
        return report_date_list

    # 遗留问题，专门生成以日期为文件的报告期数据
    def update_report(self, count, end_date, sql):
        sql += ' and a.EndDate = {0}'
        max_year = int(end_date / 10000)
        min_year = max_year - count
        report_date_list = self.create_report_date(min_year, max_year)
        for report_date in report_date_list:
            report_fundamentals = pd.read_sql(sql.format(report_date), self.source)
            if report_fundamentals.empty:
                continue
            report_fundamentals['symbol'] = np.where(report_fundamentals['Exchange'] == 'CNSESH',
                                                     report_fundamentals['code'] + '.XSHG',
                                                     report_fundamentals['code'] + '.XSHE')
            report_fundamentals.drop(['Exchange', 'code'], axis=1, inplace=True)
            # 二次加工
            if self._secondary_func is not None:
                report_fundamentals = self._secondary_func(report_fundamentals)
            # 本地保存
            if not os.path.exists(self._dir):
                os.makedirs(self._dir)
            file_name = self._dir + self.dest_table + '/' + str(report_date) + '.csv'
            if os.path.exists(str(file_name)):
                os.remove(str(file_name))
            report_fundamentals.to_csv(self._dir + self.dest_table + '/' + str(report_date) + '.csv',
                                       encoding='UTF-8')
            print(self._dir + self.dest_table + '/' + str(report_date) + '.csv')