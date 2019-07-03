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


class SyncCompanyInfo(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_COMP_INFO'
        self.dest_table = 'stk_company_info'
        super(SyncCompanyInfo, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	INT	NOT NULL,
                            symbol	VARCHAR(15)	NOT NULL,
                            pub_date	Date	DEFAULT NULL,
                            company_id	VARCHAR(10)	NOT NULL,
                            full_name	VARCHAR(200)	DEFAULT NULL,
                            short_name	VARCHAR(100)	DEFAULT NULL,
                            english_name_full	VARCHAR(300)	DEFAULT NULL,
                            english_name	VARCHAR(300)	DEFAULT NULL,
                            type1	VARCHAR(10)	DEFAULT NULL,
                            type2	VARCHAR(10)	DEFAULT NULL,
                            islist	INT	DEFAULT NULL,
                            isbranche	INT	DEFAULT NULL,
                            establish_date	Date	DEFAULT NULL,
                            type	VARCHAR(10)	DEFAULT NULL,
                            reg_capital	NUMERIC(19,2)	DEFAULT NULL,
                            auth_share	NUMERIC(19,0)	DEFAULT NULL,
                            currency	VARCHAR(10)	DEFAULT NULL,
                            org_code	VARCHAR(20)	DEFAULT NULL,
                            region	VARCHAR(10)	DEFAULT NULL,
                            country	VARCHAR(10)	DEFAULT NULL,
                            chairman	VARCHAR(100)	DEFAULT NULL,
                            ceo	VARCHAR(100)	DEFAULT NULL,
                            leger	VARCHAR(100)	DEFAULT NULL,
                            secretary	VARCHAR(50)	DEFAULT NULL,
                            secretary_phone	VARCHAR(100)	DEFAULT NULL,
                            secretary_email	VARCHAR(100)	DEFAULT NULL,
                            security_representative	VARCHAR(50)	DEFAULT NULL,
                            lawfirm	VARCHAR(100)	DEFAULT NULL,
                            cpafirm	VARCHAR(100)	DEFAULT NULL,
                            business_scale	VARCHAR(10)	DEFAULT NULL,
                            register_location	VARCHAR(200)	DEFAULT NULL,
                            zipcode	VARCHAR(20)	DEFAULT NULL,
                            office	VARCHAR(200)	DEFAULT NULL,
                            telephone	VARCHAR(100)	DEFAULT NULL,
                            fax	VARCHAR(100)	DEFAULT NULL,
                            email	VARCHAR(100)	DEFAULT NULL,
                            website	VARCHAR(100)	DEFAULT NULL,
                            pub_url	VARCHAR(100)	DEFAULT NULL,
                            description	TEXT	DEFAULT NULL,
                            business_scope	TEXT	DEFAULT NULL,
                            main_business	TEXT	DEFAULT NULL,
                            license_number	VARCHAR(50)	DEFAULT NULL,
                            live_status	VARCHAR(10)	DEFAULT NULL,
                            live_begindate	Date	DEFAULT NULL,
                            live_enddate	Date	DEFAULT NULL,
                            is_valid	INT	NOT NULL,
                            entry_date	DATE	NOT NULL,
                            entry_time	VARCHAR(8)	NOT NULL,
                            total_employees	INT	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    b.Symbol as code,
                    b.Exchange,
                    a.PUBLISHDATE as pub_date,
                    a.COMPCODE as company_id,
                    a.COMPNAME as full_name,
                    a.COMPSNAME as short_name,
                    a.ENGNAME as english_name_full,
                    a.COMPSNAME as english_name,
                    a.COMPTYPE1 as type1,
                    a.COMPTYPE2 as type2,
                    a.ISLIST as islist,
                    a.ISBRANCH as isbranche,
                    a.FOUNDDATE as establish_date,
                    a.ORGTYPE as type,
                    a.REGCAPITAL as reg_capital,
                    a.AUTHCAPSK as auth_share,
                    a.CUR as currency,
                    a.ORGCODE as org_code,
                    a.REGION as region,
                    a.COUNTRY as country,
                    a.CHAIRMAN as chairman,
                    a.MANAGER as ceo,
                    a.LEGREP as leger,
                    a.BSECRETARY as secretary,
                    a.BSECRETARYTEL as secretary_phone,
                    a.BSECRETARYMAIL as secretary_email,
                    a.SEAFFREPR as security_representative,
                    a.LECONSTANT as lawfirm,
                    a.ACCFIRM as cpafirm,
                    a.BIZSCALE as  business_scale,
                    a.REGADDR as register_location,
                    a.REGPTCODE as zipcode,
                    a.OFFICEADDR as office,
                    a.COMPTEL as telephone,
                    a.COMPFAX as fax,
                    a.COMPEMAIL as email,
                    a.COMPURL as website,
                    a.DISURL as pub_url,
                    a.COMPINTRO as description,
                    a.BIZSCOPE as business_scope,
                    a.MAJORBIZ as main_business,
                    a.BIZLICENSENO as  license_number,
                    a.COMPSTATUS as live_status,
                    a.EXISTBEGDATE as live_begindate,
                    a.EXISTENDDATE as live_enddate,
                    a.ISVALID as is_valid,
                    a.ENTRYDATE as entry_date,
                    a.ENTRYTIME as entry_time,
                    a.WORKFORCE as total_employees,
                    cast(a.tmstamp as bigint) as tmstamp
                    from {1} a 
                    left join FCDB.dbo.SecurityCode as b
                    on b.CompanyCode = a.COMPCODE
                    where b.SType ='EQA' and b.Enabled=0 and b.status=0  """
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
        processor = SyncCompanyInfo()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncCompanyInfo()
        processor.do_update()
