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


class SyncStkFinForcast(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_SK_EXPTPERFORMANCE'
        self.dest_table = 'stk_fin_forcast'
        super(SyncStkFinForcast, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	INT	NOT NULL,
                            symbol	VARCHAR(20)	NOT NULL,
                            company_id	VARCHAR(10)	NOT NULL,
                            pub_date	DATE	NOT NULL,
                            source	VARCHAR(10)	NOT NULL,
                            begin_date	DATE	NOT NULL,
                            end_date	DATE	NOT NULL,
                            base_begin_date	DATE	NOT NULL,
                            base_end_date	DATE	NOT NULL,
                            operating_income_estimate	NUMERIC(18,6)	DEFAULT NULL,
                            operating_income_increas_estimate	NUMERIC(15,6)	DEFAULT NULL,
                            operating_income_text	VARCHAR(400)	DEFAULT NULL,
                            operating_income_mark	VARCHAR(10)	DEFAULT NULL,
                            operating_profit_estimate	NUMERIC(18,6)	DEFAULT NULL,
                            operating_profit_increase_estimate	NUMERIC(15,6)	DEFAULT NULL,
                            operating_profit_text	VARCHAR(400)	DEFAULT NULL,
                            operating_profit_mark	VARCHAR(10)	DEFAULT NULL,
                            net_profit_top	NUMERIC(18,6)	DEFAULT NULL,
                            net_profit_bottom	NUMERIC(18,6)	DEFAULT NULL,
                            net_profit_increas_top	NUMERIC(18,6)	DEFAULT NULL,
                            net_profit_increas_bottom	NUMERIC(18,6)	DEFAULT NULL,
                            net_profit_estimate_top	VARCHAR(10)	DEFAULT NULL,
                            net_profit_estimate_bottom	VARCHAR(10)	DEFAULT NULL,
                            net_profit_estimate_text	VARCHAR(400)	DEFAULT NULL,
                            eps_top	NUMERIC(15,6)	DEFAULT NULL,
                            eps_bottom	NUMERIC(15,6)	DEFAULT NULL,
                            eps_estimate_top	VARCHAR(10)	DEFAULT NULL,
                            eps_estimate_bottom	VARCHAR(10)	DEFAULT NULL,
                            isvalid	INT	DEFAULT NULL,
                            entry_date	DATE	NOT NULL,
                            entry_time	VARCHAR(8)	NOT NULL,
                            currency	VARCHAR(10)	DEFAULT NULL,
                            eps_increase_estimate_top	NUMERIC(15,6)	DEFAULT NULL,
                            eps_increase_estimate_bottom	NUMERIC(15,6)	DEFAULT NULL,
                            exp_year	VARCHAR(4)	DEFAULT NULL,
                            exp_type	VARCHAR(10)	DEFAULT NULL,
                            report_type	VARCHAR(10)	DEFAULT NULL,
                            estimate_origin_text	TEXT	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`,`pub_date`,`source`,`begin_date`,`end_date`,`base_begin_date`,`base_end_date`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    a.COMPCODE as company_id,
                    a.PUBLISHDATE as pub_date,
                    a.DATASOURCE as source,
                    a.SESSIONBEGDATE as begin_date,
                    a.SESSIONENDDATE as end_date,
                    a.BASESSIONBEGDATE as base_begin_date,
                    a.BASESSIONENDDATE as base_end_date,
                    a.OPERMINCOME as operating_income_estimate,
                    a.OPERMINCOMEINC as operating_income_increas_estimate,
                    a.OPERMINCOMEDES as operating_income_text,
                    a.OPERMINCOMEMK as operating_income_mark,
                    a.OPERMPROFIT as operating_profit_estimate,
                    a.OPERMPROFITINC as operating_profit_increase_estimate,
                    a.OPERMPROFITDES as operating_profit_text,
                    a.OPERMPROFITMK as operating_profit_mark,
                    a.RETAMAXPROFITS as net_profit_top,
                    a.RETAMINPROFITS as net_profit_bottom,
                    a.RETAMAXPROFITSINC as net_profit_increas_top,
                    a.RETAMINPROFITSINC as net_profit_increas_bottom,
                    a.RETAMAXPROFITSMK as net_profit_estimate_top,
                    a.RETAMINPROFITSMK as net_profit_estimate_bottom,
                    a.RETAPROFITSDES as net_profit_estimate_text,
                    a.EPSMAXFORE as eps_top,
                    a.EPSMINFORE as eps_bottom,
                    a.EPSMAXFOREMK as eps_estimate_top,
                    a.EPSMINFOREMK as eps_estimate_bottom,
                    a.ISVALID as isvalid,
                    a.ENTRYDATE as entry_date,
                    a.ENTRYTIME as entry_time,
                    a.CUR as currency,
                    a.EPSMAXFOREINC as eps_increase_estimate_top,
                    a.EPSMINFOREINC as eps_increase_estimate_bottom,
                    a.EXPTYEAR as exp_year,
                    a.EXPTTYPE as exp_type,
                    a.GLOBALEXPTMOD as report_type,
                    a.EXPTORIGTEXT as estimate_origin_text,
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
        processor = SyncStkFinForcast()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkFinForcast()
        processor.do_update()
