#!/usr/bin/env python
# coding=utf-8
import argparse
from datetime import datetime
import pdb
import sqlalchemy as sa
import numpy as np
import pandas as pd
from sqlalchemy.orm import sessionmaker

import sys

sys.path.append('..')
sys.path.append('../..')
from sync.base_sync import BaseSync
from sync.tm_import_utils import TmImportUtils


class SyncStkShareholderTop10(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_SK_SHAREHOLDER'
        self.dest_table = 'stk_shareholder_top10'
        super(SyncStkShareholderTop10, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	INT	NOT NULL,
                            symbol	VARCHAR(20)	NOT NULL,
                            pub_date	DATE	NOT NULL,
                            end_date	DATE	NOT NULL,
                            company_id	VARCHAR(10)	NOT NULL,
                            shareholder_id	VARCHAR(10)	DEFAULT NULL,
                            shareholder_name	VARCHAR(200)	NOT NULL,
                            shareholder_class	VARCHAR(10)	NOT NULL,
                            sharesnature	VARCHAR(10)	NOT NULL,
                            shareholder_rank	NUMERIC(10,0)	NOT NULL,
                            sharesnature_id	VARCHAR(100)	DEFAULT NULL,
                            share_number	NUMERIC(26,2)	DEFAULT NULL,
                            share_ratio	NUMERIC(8,4)	DEFAULT NULL,
                            share_pledge_freeze	NUMERIC(26,2)	DEFAULT NULL,
                            share_change	NUMERIC(26,2)	DEFAULT NULL,
                            is_history	INT	DEFAULT NULL,
                            update_date	DATE	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`,`end_date`,`shareholder_name`,`shareholder_rank`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    a.PUBLISHDATE as pub_date,
                    a.ENDDATE as end_date,
                    a.COMPCODE as company_id,
                    a.SHHOLDERCODE as shareholder_id,
                    a.SHHOLDERNAME as shareholder_name,
                    a.SHHOLDERTYPE as shareholder_class,
                    a.SHHOLDERNATURE as sharesnature,
                    a.RANK as shareholder_rank,
                    a.SHARESTYPE as sharesnature_id,
                    a.HOLDERAMT as share_number,
                    a.HOLDERRTO as share_ratio,
                    a.PFHOLDERAMT as share_pledge_freeze,
                    a.CURCHG as share_change,
                    a.ISHIS as is_history,
                    a.UPDATEDATE as update_date,
                    cast(a.tmstamp as bigint) as tmstamp,
                    b.Symbol as code,
                    b.Exchange
                    from {1} a 
                    left join FCDB.dbo.SecurityCode as b
                    on b.CompanyCode = a.COMPCODE
                    where b.SType ='EQA' and b.Enabled=0  and b.Status=0 """
        if type == 'report':
            sql = sql.format('', self.source_table)
            return sql
        elif type == 'update':
            sql += 'and cast(a.tmstamp as bigint) > {2} order by a.tmstamp'
            return sql

    def get_datas(self, tm):
        print('正在查询', self.source_table, '表大于', tm, '的数据')
        sql = self.get_sql('update')
        sql = sql.format('top 10000', self.source_table, tm).replace('\n', '')
        trades_sets = pd.read_sql(sql, self.source)
        return trades_sets

    def update_table_data(self, tm):
        while True:
            result_list = self.get_datas(tm)
            if not result_list.empty:
                result_list['symbol'] = np.where(result_list['Exchange'] == 'CNSESH',
                                                 result_list['code'] + '.XSHG',
                                                 result_list['code'] + '.XSHE')
                result_list.drop(['Exchange', 'code'], axis=1, inplace=True)
                try:
                    result_list.to_sql(name=self.dest_table, con=self.destination, if_exists='append', index=False)
                except Exception as e:
                    print(e.orig.msg)
                    self.insert_or_update(result_list)
                max_tm = result_list['tmstamp'][result_list['tmstamp'].size - 1]
                self.utils.update_update_log(max_tm)
                tm = max_tm
            else:
                break

    def do_update(self):
        max_tm = self.utils.get_max_tm_source()
        log_tm = self.utils.get_max_tm_log()
        if max_tm > log_tm:
            self.update_table_data(log_tm)

    def update_report(self, count, end_date):
        self.utils.update_report(count, end_date, self.get_sql('report'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    args = parser.parse_args()
    if args.rebuild:
        processor = SyncStkShareholderTop10()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkShareholderTop10()
        processor.do_update()
