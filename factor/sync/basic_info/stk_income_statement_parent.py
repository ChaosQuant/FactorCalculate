#!/usr/bin/env python
# coding=utf-8
import argparse
from datetime import datetime
import pdb
import sqlalchemy as sa
import pandas as pd
import numpy as np
from sqlalchemy.orm import sessionmaker
import sys

sys.path.append('..')
sys.path.append('../..')
from sync.base_sync import BaseSync
from sync.tm_import_utils import TmImportUtils


class SyncStkIncomeStatementParent(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_FIN_PROINCSTATEMENTNEW'
        self.dest_table = 'stk_income_statement_parent'
        super(SyncStkIncomeStatementParent, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	VARCHAR(12)	NOT NULL,
                            company_id	VARCHAR(12)	NOT NULL,
                            company_name	VARCHAR(50)	CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'	NOT NULL,
                            symbol	VARCHAR(12)	NOT NULL,
                            report_type	VARCHAR(2)	NOT NULL,
                            report_date	VARCHAR(10)	NOT NULL,
                            pub_date	DATE	NOT NULL,
                            start_date	DATE	NOT NULL,
                            end_date	DATE	NOT NULL,
                            source	VARCHAR(10)	NOT NULL,
                            total_operating_revenue	NUMERIC(26,2)	DEFAULT NULL,
                            operating_revenue	NUMERIC(26,2)	DEFAULT NULL,
                            total_operating_cost	NUMERIC(26,2)	DEFAULT NULL,
                            operating_cost	NUMERIC(26,2)	DEFAULT NULL,
                            operating_tax_surcharges	NUMERIC(26,2)	DEFAULT NULL,
                            sale_expense	NUMERIC(26,2)	DEFAULT NULL,
                            administration_expense	NUMERIC(26,2)	DEFAULT NULL,
                            financial_expense	NUMERIC(26,2)	DEFAULT NULL,
                            asset_impairment_loss	NUMERIC(26,2)	DEFAULT NULL,
                            fair_value_variable_income	NUMERIC(26,2)	DEFAULT NULL,
                            investment_income	NUMERIC(26,2)	DEFAULT NULL,
                            invest_income_associates	NUMERIC(26,2)	DEFAULT NULL,
                            exchange_income	NUMERIC(26,2)	DEFAULT NULL,
                            operating_profit	NUMERIC(26,2)	DEFAULT NULL,
                            subsidy_income	NUMERIC(26,2)	DEFAULT NULL,
                            non_operating_revenue	NUMERIC(26,2)	DEFAULT NULL,
                            non_operating_expense	NUMERIC(26,2)	DEFAULT NULL,
                            disposal_loss_non_current_liability	NUMERIC(26,2)	DEFAULT NULL,
                            total_profit	NUMERIC(26,2)	DEFAULT NULL,
                            income_tax	NUMERIC(26,2)	DEFAULT NULL,
                            net_profit	NUMERIC(26,2)	DEFAULT NULL,
                            np_parent_company_owners	NUMERIC(26,2)	DEFAULT NULL,
                            minority_profit	NUMERIC(26,2)	DEFAULT NULL,
                            basic_eps	NUMERIC(30,6)	DEFAULT NULL,
                            diluted_eps	NUMERIC(30,6)	DEFAULT NULL,
                            other_composite_income	NUMERIC(26,2)	DEFAULT NULL,
                            total_composite_income	NUMERIC(26,2)	DEFAULT NULL,
                            ci_parent_company_owners	NUMERIC(26,2)	DEFAULT NULL,
                            ci_minority_owners	NUMERIC(26,2)	DEFAULT NULL,
                            interest_income	NUMERIC(26,2)	DEFAULT NULL,
                            premiums_earned	NUMERIC(26,2)	DEFAULT NULL,
                            commission_income	NUMERIC(26,2)	DEFAULT NULL,
                            interest_expense	NUMERIC(26,2)	DEFAULT NULL,
                            commission_expense	NUMERIC(26,2)	DEFAULT NULL,
                            refunded_premiums	NUMERIC(26,2)	DEFAULT NULL,
                            net_pay_insurance_claims	NUMERIC(26,2)	DEFAULT NULL,
                            withdraw_insurance_contract_reserve	NUMERIC(26,2)	DEFAULT NULL,
                            policy_dividend_payout	NUMERIC(26,2)	DEFAULT NULL,
                            reinsurance_cost	NUMERIC(26,2)	DEFAULT NULL,
                            non_current_asset_disposed	NUMERIC(26,2)	DEFAULT NULL,
                            other_earnings	NUMERIC(26,2)	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`,`start_date`,`end_date`,`report_type`)
                        )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self.dest_table)
        create_sql = create_sql.replace('\n', '')
        self.create_table(create_sql)

    def get_sql(self, type):
        sql = """select {0} 
                    a.ID as id,
                    a.COMPCODE as company_id,
                    b.SName as company_name,
                    b.Symbol as code,
                    b.Exchange,
                    a.PUBLISHDATE as pub_date,
                    a.BEGINDATE as start_date,
                    a.ENDDATE as end_date,
                    a.REPORTDATETYPE as report_date,
                    a.REPORTTYPE as report_type,
                    a.DATASOURCE as source,
                    a.BIZTOTINCO as total_operating_revenue,
                    a.BIZINCO as operating_revenue,
                    a.BIZTOTCOST as total_operating_cost,
                    a.BIZCOST as operating_cost,
                    a.BIZTAX as operating_tax_surcharges,
                    a.SALESEXPE as sale_expense,
                    a.MANAEXPE as administration_expense,
                    a.FINEXPE as financial_expense,
                    a.ASSEIMPALOSS as asset_impairment_loss,
                    a.VALUECHGLOSS as fair_value_variable_income,
                    a.INVEINCO as investment_income,
                    a.ASSOINVEPROF as invest_income_associates,
                    a.EXCHGGAIN as exchange_income,
                    a.PERPROFIT as operating_profit,
                    a.SUBSIDYINCOME as subsidy_income,
                    a.NONOREVE as non_operating_revenue,
                    a.NONOEXPE as non_operating_expense,
                    a.NONCASSETSDISL as disposal_loss_non_current_liability,
                    a.TOTPROFIT as total_profit,
                    a.INCOTAXEXPE as income_tax,
                    a.NETPROFIT as net_profit,
                    a.PARENETP as np_parent_company_owners,
                    a.MINYSHARRIGH as minority_profit,
                    a.BASICEPS as basic_eps,
                    a.DILUTEDEPS as diluted_eps,
                    a.OTHERCOMPINCO as other_composite_income,
                    a.COMPINCOAMT as total_composite_income,
                    a.PARECOMPINCOAMT as ci_parent_company_owners,
                    a.MINYSHARINCOAMT as ci_minority_owners,
                    a.INTEINCO as interest_income,
                    a.EARNPREM as premiums_earned,
                    a.POUNINCO as commission_income,
                    a.INTEEXPE as interest_expense,
                    a.POUNEXPE as commission_expense,
                    a.SURRGOLD as refunded_premiums,
                    a.COMPNETEXPE as net_pay_insurance_claims,
                    a.CONTRESS as withdraw_insurance_contract_reserve,
                    a.POLIDIVIEXPE as policy_dividend_payout,
                    a.REINEXPE as reinsurance_cost,
                    a.NONCASSETSDISI as non_current_asset_disposed,
                    a.OTHERINCO as other_earnings,
                    cast(a.tmstamp as bigint) as tmstamp
                    from {1} a 
                    left join FCDB.dbo.SecurityCode as b
                    on b.CompanyCode = a.COMPCODE
                    where REPORTTYPE in ('2','4')  and b.SType ='EQA' and b.Enabled=0 and b.status=0 and a.ACCSTACODE=11002"""
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
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--report', type=bool, default=False)
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = SyncStkIncomeStatementParent()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkIncomeStatementParent()
        processor.do_update()
    elif args.report:
        processor = SyncStkIncomeStatementParent()
        processor.update_report(args.count, end_date)
