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


class SyncSecurityInfo(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_SK_BASICINFO'
        self.dest_table = 'stk_security_info'
        super(SyncSecurityInfo,self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	INT	NOT NULL,
                            company_id	VARCHAR(20)	NOT NULL,
                            symbol	VARCHAR(20)	NOT NULL,
                            exchange	VARCHAR(10)	NOT NULL,
                            security_type	VARCHAR(10)	NOT NULL,
                            short_name	VARCHAR(100)	NOT NULL,
                            english_name	VARCHAR(100)	DEFAULT NULL,
                            decnum	INT	DEFAULT NULL,
                            currency	VARCHAR(10)	NOT NULL,
                            isin_code	VARCHAR(20)	DEFAULT NULL,
                            sedol_code	VARCHAR(20)	DEFAULT NULL,
                            pairvalue	NUMERIC(19,2)	DEFAULT NULL,
                            total_shares	NUMERIC(15,6)	DEFAULT NULL,
                            lists_tatus	VARCHAR(10)	NOT NULL,
                            list_date	DATE	NOT NULL,
                            ipo_price	NUMERIC(9,4)	DEFAULT NULL,
                            delist_date	DATE	NOT NULL,
                            delist_price	NUMERIC(9,4)	DEFAULT NULL,
                            sfc_industry1_code	VARCHAR(10)	DEFAULT NULL,
                            sfc_industry1_name	VARCHAR(100)	DEFAULT NULL,
                            sfc_industry2_code	VARCHAR(10)	DEFAULT NULL,
                            sfc_industry2_name	VARCHAR(100)	DEFAULT NULL,
                            gics_industry1_code	VARCHAR(10)	DEFAULT NULL,
                            gics_industry1_name	VARCHAR(100)	DEFAULT NULL,
                            gics_industry2_code	VARCHAR(10)	DEFAULT NULL,
                            gics_industry2_name	VARCHAR(100)	DEFAULT NULL,
                            sw_industry1_code	VARCHAR(10)	DEFAULT NULL,
                            sw_industry1_name	VARCHAR(100)	DEFAULT NULL,
                            sw_industry2_code	VARCHAR(10)	DEFAULT NULL,
                            sw_industry2_name	VARCHAR(100)	DEFAULT NULL,
                            csi_industry1_code	VARCHAR(10)	DEFAULT NULL,
                            csi_industry1_name	VARCHAR(100)	DEFAULT NULL,
                            csi_industry2_code	VARCHAR(10)	DEFAULT NULL,
                            csi_industry2_name	VARCHAR(100)	DEFAULT NULL,
                            province_code	VARCHAR(10)	DEFAULT NULL,
                            province_name	VARCHAR(100)	DEFAULT NULL,
                            city_code	VARCHAR(10)	DEFAULT NULL,
                            city_name	VARCHAR(100)	DEFAULT NULL,
                            isvalid	INT	DEFAULT NULL,
                            entry_date	DATE	NOT NULL,
                            entry_time	VARCHAR(8)	DEFAULT NULL,
                            value_currency	VARCHAR(10)	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    a.COMPCODE as company_id,
                    a.SYMBOL as code,
                    a.EXCHANGE as exchange,
                    a.SETYPE as security_type,
                    a.SESNAME as short_name,
                    a.SEENGNAME as english_name,
                    a.DECNUM as decnum,
                    a.CUR as currency,
                    a.ISINCODE as isin_code,
                    a.SEDOLCODE as sedol_code,
                    a.PARVALUE as pairvalue,
                    a.TOTALSHARE as total_shares,
                    a.LISTSTATUS as lists_tatus,
                    a.LISTDATE as list_date,
                    a.LISTOPRICE as ipo_price,
                    a.DELISTDATE as delist_date,
                    a.DELISTCPRICE as delist_price,
                    a.CSRCLEVEL1CODE as sfc_industry1_code,
                    a.CSRCLEVEL1NAME as sfc_industry1_name,
                    a.CSRCLEVEL2CODE as sfc_industry2_code,
                    a.CSRCLEVEL2NAME as sfc_industry2_name,
                    a.GICSLEVEL1CODE as gics_industry1_code,
                    a.GICSLEVEL1NAME as gics_industry1_name,
                    a.GICSLEVEL2CODE as gics_industry2_code,
                    a.GICSLEVEL2NAME as gics_industry2_name,
                    a.SWLEVEL1CODE as sw_industry1_code,
                    a.SWLEVEL1NAME as sw_industry1_name,
                    a.SWLEVEL2CODE as sw_industry2_code,
                    a.SWLEVEL2NAME as sw_industry2_name,
                    a.CSILEVEL1CODE as csi_industry1_code,
                    a.CSILEVEL1NAME as csi_industry1_name,
                    a.CSILEVEL2CODE as csi_industry2_code,
                    a.CSILEVEL2NAME as csi_industry2_name,
                    a.PROVINCECODE as province_code,
                    a.PROVINCENAME as province_name,
                    a.CITYCODE as city_code,
                    a.CITYNAME as city_name,
                    a.ISVALID as isvalid,
                    a.ENTRYDATE as entry_date,
                    a.ENTRYTIME as entry_time,
                    a.VALUECUR as value_currency,
                    cast(a.tmstamp as bigint) as tmstamp
                    from {1} a 
                    where a.LISTSTATUS=1 and (a.EXCHANGE='001002' or a.EXCHANGE='001003') """
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
                result_list['symbol'] = np.where(result_list['exchange'] == '001002',
                                                 result_list['code'] + '.XSHG',
                                                 result_list['code'] + '.XSHE')
                result_list.drop(['code'], axis=1, inplace=True)
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
        processor = SyncSecurityInfo()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncSecurityInfo()
        processor.do_update()
