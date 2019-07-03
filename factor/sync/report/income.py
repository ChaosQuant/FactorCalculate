#!/usr/bin/env python
# coding=utf-8
import collections
import numpy as np
import pdb
from datetime import datetime
import sqlalchemy as sa
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import argparse
from sync_fundamentals import SyncFundamentals


class SyncIncome(object):
    def __init__(self):
        self._sync_fun = SyncFundamentals(None, None, 'income')

    def create_dest_tables(self):
        self._sync_fun.create_dest_tables('income')

    def create_dest_report_tables(self):
        self._sync_fun.create_dest_report_tables('income_report')

    def create_columns(self):
        columns_list = collections.OrderedDict()
        columns_list["total_operating_revenue"] = "decimal(19,4)"
        columns_list["operating_revenue"] = "decimal(19,4)"
        columns_list["interest_income"] = "decimal(19,4)"
        columns_list["premiums_earned"] = "decimal(19,4)"
        columns_list["commission_income"] = "decimal(19,4)"
        columns_list["total_operating_cost"] = "decimal(19,4)"
        columns_list["operating_cost"] = "decimal(19,4)"
        columns_list["interest_expense"] = "decimal(19,4)"
        columns_list["commission_expense"] = "decimal(19,4)"
        columns_list["refunded_premiums"] = "decimal(19,4)"
        columns_list["net_pay_insurance_claims"] = "decimal(19,4)"
        columns_list["withdraw_insurance_contract_reserve"] = "decimal(19,4)"
        columns_list["policy_dividend_payout"] = "decimal(19,4)"
        columns_list["reinsurance_cost"] = "decimal(19,4)"
        columns_list["operating_tax_surcharges"] = "decimal(19,4)"
        columns_list["sale_expense"] = "decimal(19,4)"
        columns_list["administration_expense"] = "decimal(19,4)"
        columns_list["financial_expense"] = "decimal(19,4)"
        columns_list["asset_impairment_loss"] = "decimal(19,4)"
        columns_list["fair_value_variable_income"] = "decimal(19,4)"
        columns_list["investment_income"] = "decimal(19,4)"
        columns_list["invest_income_associates"] = "decimal(19,4)"
        columns_list["exchange_income"] = "decimal(19,4)"
        columns_list["operating_profit"] = "decimal(19,4)"
        columns_list["non_operating_revenue"] = "decimal(19,4)"
        columns_list["non_operating_expense"] = "decimal(19,4)"
        columns_list["disposal_loss_non_current_liability"] = "decimal(19,4)"
        columns_list["total_profit"] = "decimal(19,4)"
        columns_list["income_tax_expense"] = "decimal(19,4)"
        columns_list["net_profit"] = "decimal(19,4)"
        columns_list["np_parent_company_owners"] = "decimal(19,4)"
        columns_list["minority_profit"] = "decimal(19,4)"
        columns_list["basic_eps"] = "decimal(19,4)"
        columns_list["diluted_eps"] = "decimal(19,4)"
        columns_list["other_composite_income"] = "decimal(19,4)"
        columns_list["total_composite_income"] = "decimal(19,4)"
        columns_list["ci_parent_company_owners"] = "decimal(19,4)"
        columns_list["ci_minority_owners"] = "decimal(19,4)"

        columns_list = collections.OrderedDict(sorted(columns_list.items(), key=lambda t: t[0]))
        time_columns = 'P.ENDDATE'
        del_columns = ['code', 'EXCHANGE', 'SType', 'ReportStyle', 'year']
        sub_columns = ['total_operating_revenue', 'operating_revenue', 'interest_income', 'premiums_earned',
                       'commission_income', 'total_operating_cost', 'operating_cost', 'interest_expense',
                       'commission_expense', 'refunded_premiums', 'net_pay_insurance_claims',
                       'withdraw_insurance_contract_reserve', 'policy_dividend_payout', 'reinsurance_cost',
                       'operating_tax_surcharges', 'sale_expense', 'administration_expense', 'financial_expense',
                       'asset_impairment_loss', 'fair_value_variable_income', 'investment_income',
                       'invest_income_associates', 'exchange_income', 'operating_profit', 'non_operating_revenue',
                       'non_operating_expense', 'disposal_loss_non_current_liability', 'total_profit',
                       'income_tax_expense', 'net_profit', 'np_parent_company_owners', 'minority_profit', 'basic_eps',
                       'diluted_eps', 'other_composite_income', 'total_composite_income', 'ci_parent_company_owners',
                       'ci_minority_owners']  # 需要拿出来算单季
        self._sync_fun.set_columns(columns_list, self.create_sql(), time_columns, del_columns, sub_columns)
        self._sync_fun.set_change_symbol(self.change_symbol)

    def create_sql(self):
        sql = """select S.Symbol AS code,S.Exchange AS EXCHANGE, S.SType, P.PublishDate AS pub_date,
                    P.ENDDATE AS report_date,P.REPORTTYPE AS ReportStyle, REPORTYEAR as year,
                    P.BIZTOTINCO as total_operating_revenue,
                    P.BIZINCO as operating_revenue,
                    P.INTEINCO as interest_income,
                    P.EARNPREM as premiums_earned,
                    P.POUNINCO as commission_income,
                    P.BIZTOTCOST as total_operating_cost,
                    P.BIZCOST as operating_cost,
                    P.INTEEXPE as interest_expense,
                    P.POUNEXPE as commission_expense,
                    P.SURRGOLD as refunded_premiums,
                    P.COMPNETEXPE as net_pay_insurance_claims,
                    P.CONTRESS as withdraw_insurance_contract_reserve,
                    P.POLIDIVIEXPE as policy_dividend_payout,
                    P.REINEXPE as reinsurance_cost,
                    P.BIZTAX as operating_tax_surcharges,
                    P.SALESEXPE as sale_expense,
                    P.MANAEXPE as administration_expense,
                    P.FINEXPE as financial_expense,
                    P.ASSEIMPALOSS as asset_impairment_loss,
                    P.VALUECHGLOSS as fair_value_variable_income,
                    P.INVEINCO as investment_income,
                    P.ASSOINVEPROF as invest_income_associates,
                    P.EXCHGGAIN as exchange_income,
                    P.PERPROFIT as operating_profit,
                    P.NONOREVE as non_operating_revenue,
                    P.NONOEXPE as non_operating_expense,
                    P.NONCASSETSDISL as disposal_loss_non_current_liability,
                    P.TOTPROFIT as total_profit,
                    P.INCOTAXEXPE as income_tax_expense,
                    P.NETPROFIT as net_profit,
                    P.PARENETP as np_parent_company_owners,
                    P.MINYSHARRIGH as minority_profit,
                    P.BASICEPS as basic_eps,
                    P.DILUTEDEPS as diluted_eps,
                    P.OTHERCOMPINCO as other_composite_income,
                    P.COMPINCOAMT as total_composite_income,
                    P.PARECOMPINCO as ci_parent_company_owners,
                    P.MINYSHARINCO as ci_minority_owners
                    from QADB.dbo.TQ_FIN_PROINCSTATEMENTNEW AS P JOIN FCDB.dbo.SecurityCode as S on
                    S.CompanyCode = P.COMPCODE
                    where P.REPORTTYPE={0} AND S.SType='{1}' and S.Enabled=0 and S.Status=0  AND """.format(1,
                                                                                                            'EQA')
        return sql

    def change_symbol(self, trades_date_df):
        return np.where(trades_date_df['EXCHANGE'] == 'CNSESH',
                        trades_date_df['code'] + '.XSHG',
                        trades_date_df['code'] + '.XSHE')

    def update_report(self, start_date, end_date, count):
        self._sync_fun.update_report(start_date, end_date, count)

    def do_update(self, start_date, end_date, count, order='DESC'):
        self._sync_fun.do_update(start_date, end_date, count, order)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=2)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--report', type=bool, default=False)
    parser.add_argument('--rebuild_report', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = SyncIncome()
        processor.create_columns()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.update:
        processor = SyncIncome()
        processor.create_columns()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.rebuild_report:
        processor = SyncIncome()
        processor.create_columns()
        processor.create_dest_report_tables()
        processor.update_report(args.start_date, end_date, args.count)
    elif args.report:
        processor = SyncIncome()
        processor.create_columns()
        processor.update_report(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncIncome()
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name, 'trade_date')
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name + '_report', 'report_date')
        print('running schedule report task, start date:', start_date, ';end date:', end_date)
        processor.update_report(start_date, end_date, -1)