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
from base_sync import BaseSync
from sync.tm_import_utils import TmImportUtils
import config


class SyncStkCapitalChange(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_SK_SHARESTRUCHG'
        self.dest_table = 'stk_capital_change'
        super(SyncStkCapitalChange, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	INT,
                            symbol	VARCHAR(20),
                            company_id	VARCHAR(10),
                            pub_date	Date,
                            begin_date	Date,
                            end_date	Date,
                            total_shares	NUMERIC(15,6),
                            floating_shares	NUMERIC(15,6),
                            floating_ashares	NUMERIC(15,6),
                            floating_bshares	NUMERIC(15,6),
                            floating_hshares	NUMERIC(15,6),
                            other_floating_shares	NUMERIC(15,6),
                            restrict_floating_shares	NUMERIC(15,6),
                            restrict_floating_ashares	NUMERIC(15,6),
                            non_floating_ashares	NUMERIC(15,6),
                            free_floating_shares	NUMERIC(15,6),
                            b_shares	NUMERIC(15,6),
                            exdividend_date	Date,
                            `explain`	VARCHAR(100),
                            change_type	VARCHAR(2),
                            change_reason	VARCHAR(1000),
                            is_valid	INT,
                            entry_date	DATE,
                            entry_time VARCHAR(8),
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`,`begin_date`,`exdividend_date`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    a.COMPCODE as company_id,
                    a.PUBLISHDATE as pub_date,
                    a.BEGINDATE as begin_date,
                    a.ENDDATE as end_date,
                    a.TOTALSHARE as total_shares,
                    a.CIRCSKAMT as floating_shares,
                    a.CIRCAAMT as floating_ashares,
                    a.CIRCBAMT as floating_bshares,
                    a.CIRCHAMT as floating_hshares,
                    a.OTHERCIRCAMT as other_floating_shares,
                    a.LIMSKAMT as restrict_floating_shares,
                    a.RECIRCAAMT as restrict_floating_ashares,
                    a.NCIRCAMT as non_floating_ashares,
                    a.FCIRCAAMT as free_floating_shares,
                    a.BSK as b_shares,
                    a.EXRIGHTDATE as exdividend_date,
                    a.EXRIGHTEXP as explain,
                    a.SKCHGTYPE as change_type,
                    a.SHCHGRSN as change_reason,
                    a.ISVALID as is_valid,
                    a.ENTRYDATE as entry_date,
                    a.ENTRYTIME as entry_time,
                    cast(a.tmstamp as bigint) as tmstamp,
                    b.Symbol as code,
                    b.Exchange
                    from {1} a 
                    left join FCDB.dbo.SecurityCode as b
                    on b.CompanyCode = a.COMPCODE
                    where b.SType ='EQA' and b.Enabled=0 and b.Status=0 """
        if type == 'report':
            sql = sql.format('', self.source_table)
            return sql
        elif type == 'update':
            sql += 'and cast(a.tmstamp as bigint) > {2} order by a.tmstamp'
            return sql

    def get_datas(self, tm):
        print('正在查询', self.source_table, '表大于', tm, '的数据')
        sql = self.get_sql('update')
        sql = sql.format('top 2000', self.source_table, tm).replace('\n', '')
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
        processor = SyncStkCapitalChange()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkCapitalChange()
        processor.do_update()
