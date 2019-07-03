#!/usr/bin/env python
# coding=utf-8
import argparse
import sys
import numpy as np
import pandas as pd

sys.path.append('..')
sys.path.append('../..')
from sync.base_sync import BaseSync
from sync.tm_import_utils import TmImportUtils


class SyncStkXrXd(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_SK_PRORIGHTS'
        self.dest_table = 'stk_xr_xd'
        super(SyncStkXrXd, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	INT	NOT NULL,
                            pub_date	DATE,
                            update_date	DATE,
                            sec_code	VARCHAR(20)	NOT NULL,
                            symbol	VARCHAR(20)	NOT NULL,
                            company_id	VARCHAR(20)	NOT NULL,
                            divdence_year	VARCHAR(20)	NOT NULL,
                            date_type	VARCHAR(10)	NOT NULL,
                            divdence_type	VARCHAR(10)	NOT NULL,
                            rank_num	INT	NOT NULL,
                            issue_object_type	VARCHAR(10)	NOT NULL,
                            issue_object	VARCHAR(400)	DEFAULT NULL,
                            project_type	VARCHAR(10)	NOT NULL,
                            currency	VARCHAR(10)	NOT NULL,
                            equity_base_date	DATE,
                            equity_base	NUMERIC(19,0)	DEFAULT NULL,
                            record_date	DATE,
                            xdr_date	DATE,
                            lasttrade_date	DATE,
                            aftertax_earning	NUMERIC(19,6)	DEFAULT NULL,
                            qfii_aftertax_earning	NUMERIC(19,6)	DEFAULT NULL,
                            cash_begindate	DATE,
                            cash_enddate	DATE,
                            share_deliveryratio	NUMERIC(19,10)	DEFAULT NULL,
                            capital_transferratio	NUMERIC(19,10)	DEFAULT NULL,
                            share_donationratio	NUMERIC(19,10)	DEFAULT NULL,
                            share_arrivaldate	DATE,
                            list_date	DATE,
                            buyback_date	DATE,
                            buyback_deadline date,
                            sharereform_date	DATE,
                            meeting_pubdate	DATE,
                            is_newplan	INT	NOT NULL,
                            xdr_statement	VARCHAR(2000)	DEFAULT NULL,
                            is_valid	INT	DEFAULT NULL,
                            entry_date	DATE,
                            entry_time	VARCHAR(8)	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`,`sec_code`,`divdence_year`,`date_type`,`divdence_type`,`rank_num`,`project_type`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    b.Exchange,
                    a.PUBLISHDATE as pub_date,
                    a.UPDATEDATE as update_date,
                    a.SECODE as sec_code,
                    a.SYMBOL as code,
                    a.COMPCODE as company_id,
                    a.DIVIYEAR as divdence_year,
                    a.DATETYPE as date_type,
                    a.DIVITYPE as divdence_type,
                    a.RANKNUM as rank_num,
                    a.GRAOBJTYPE as issue_object_type,
                    a.GRAOBJ as issue_object,
                    a.PROJECTTYPE as project_type,
                    a.CUR as currency,
                    a.SHCAPBASEDATE as equity_base_date,
                    a.SHCAPBASEQTY as equity_base,
                    a.EQURECORDDATE as record_date,
                    a.XDRDATE as xdr_date,
                    a.LASTTRADDAE as lasttrade_date,
                    a.AFTTAXCASHDVCNY as aftertax_earning,
                    a.AFTTAXCASHDVCNYQFII as qfii_aftertax_earning,
                    a.CASHDVARRBEGDATE as cash_begindate,
                    a.CASHDVARRENDDATE as cash_enddate,
                    a.PROBONUSRT as share_deliveryratio,
                    a.TRANADDRT as capital_transferratio,
                    a.BONUSRT as share_donationratio,
                    a.SHARRDATE as share_arrivaldate,
                    a.LISTDATE as list_date,
                    a.REPUBEGDATE as buyback_date,
                    a.REPUENDDATE as buyback_deadline,
                    a.ASSREPLACDATE as sharereform_date,
                    a.SHHDMEETRESPUBDATE as meeting_pubdate,
                    a.ISNEWEST as is_newplan,
                    a.DIVIEXPMEMO as xdr_statement,
                    a.ISVALID as is_valid,
                    a.ENTRYDATE as entry_date,
                    a.ENTRYTIME as entry_time,
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
                result_list['symbol'] = np.where(result_list['Exchange'] == '001002',
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
        processor = SyncStkXrXd()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkXrXd()
        processor.do_update()
