#!/usr/bin/env python
# coding=utf-8
import argparse
from datetime import datetime

import sqlalchemy as sa
import numpy as np
import pandas as pd
from sqlalchemy.orm import sessionmaker
import sys

sys.path.append('..')
sys.path.append('../..')
from sync.base_sync import BaseSync
from sync.tm_import_utils import TmImportUtils


class SyncStkCashflowStatementParent(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_FIN_PROCFSTATEMENTNEW'
        self.dest_table = 'stk_cashflow_statement_parent'
        super(SyncStkCashflowStatementParent, self).__init__(self.dest_table)
        # 源数据库
        self.source = sa.create_engine("mssql+pymssql://read:read@192.168.100.64:1433/QADB?charset=GBK")

        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	VARCHAR(12)	NOT NULL,
                            company_id	VARCHAR(20)	NOT NULL,
                            company_name	VARCHAR(20)	NOT NULL,
                            symbol	VARCHAR(12)	NOT NULL,
                            pub_date	Date	NOT NULL,
                            start_date	Date	NOT NULL,
                            end_date	Date	NOT NULL,
                            report_type	VARCHAR(10)	NOT NULL,
                            report_date	VARCHAR(10)	NOT NULL,
                            source	VARCHAR(10)	NOT NULL,
                            goods_sale_and_service_render_cash	NUMERIC(26,2)	DEFAULT NULL,
                            tax_levy_refund	NUMERIC(26,2)	DEFAULT NULL,
                            subtotal_operate_cash_inflow	NUMERIC(26,2)	DEFAULT NULL,
                            goods_and_services_cash_paid	NUMERIC(26,2)	DEFAULT NULL,
                            staff_behalf_paid	NUMERIC(26,2)	DEFAULT NULL,
                            tax_payments	NUMERIC(26,2)	DEFAULT NULL,
                            subtotal_operate_cash_outflow	NUMERIC(26,2)	DEFAULT NULL,
                            net_operate_cash_flow	NUMERIC(26,2)	DEFAULT NULL,
                            invest_withdrawal_cash	NUMERIC(26,2)	DEFAULT NULL,
                            invest_proceeds	NUMERIC(26,2)	DEFAULT NULL,
                            fix_intan_other_asset_dispo_cash	NUMERIC(26,2)	DEFAULT NULL,
                            net_cash_deal_subcompany	NUMERIC(26,2)	DEFAULT NULL,
                            subtotal_invest_cash_inflow	NUMERIC(26,2)	DEFAULT NULL,
                            fix_intan_other_asset_acqui_cash	NUMERIC(26,2)	DEFAULT NULL,
                            invest_cash_paid	NUMERIC(26,2)	DEFAULT NULL,
                            impawned_loan_net_increase	NUMERIC(26,2)	DEFAULT NULL,
                            net_cash_from_sub_company	NUMERIC(26,2)	DEFAULT NULL,
                            subtotal_invest_cash_outflow	NUMERIC(26,2)	DEFAULT NULL,
                            net_invest_cash_flow	NUMERIC(26,2)	DEFAULT NULL,
                            cash_from_invest	NUMERIC(26,2)	DEFAULT NULL,
                            cash_from_borrowing	NUMERIC(26,2)	DEFAULT NULL,
                            cash_from_bonds_issue	NUMERIC(26,2)	DEFAULT NULL,
                            subtotal_finance_cash_inflow	NUMERIC(26,2)	DEFAULT NULL,
                            borrowing_repayment	NUMERIC(26,2)	DEFAULT NULL,
                            dividend_interest_payment	NUMERIC(26,2)	DEFAULT NULL,
                            subtotal_finance_cash_outflow	NUMERIC(26,2)	DEFAULT NULL,
                            net_finance_cash_flow	NUMERIC(26,2)	DEFAULT NULL,
                            exchange_rate_change_effect	NUMERIC(26,2)	DEFAULT NULL,
                            cash_equivalent_increase	NUMERIC(26,2)	DEFAULT NULL,
                            cash_equivalents_at_beginning	NUMERIC(26,2)	DEFAULT NULL,
                            cash_and_equivalents_at_end	NUMERIC(26,2)	DEFAULT NULL,
                            net_profit	NUMERIC(26,2)	DEFAULT NULL,
                            assets_depreciation_reserves	NUMERIC(26,2)	DEFAULT NULL,
                            fixed_assets_depreciation	NUMERIC(26,2)	DEFAULT NULL,
                            intangible_assets_amortization	NUMERIC(26,2)	DEFAULT NULL,
                            defferred_expense_amortization	NUMERIC(26,2)	DEFAULT NULL,
                            fix_intan_other_asset_dispo_loss	NUMERIC(26,2)	DEFAULT NULL,
                            fixed_asset_scrap_loss	NUMERIC(26,2)	DEFAULT NULL,
                            fair_value_change_loss	NUMERIC(26,2)	DEFAULT NULL,
                            financial_cost	NUMERIC(26,2)	DEFAULT NULL,
                            invest_loss	NUMERIC(26,2)	DEFAULT NULL,
                            deffered_tax_asset_decrease	NUMERIC(26,2)	DEFAULT NULL,
                            deffered_tax_liability_increase	NUMERIC(26,2)	DEFAULT NULL,
                            inventory_decrease	NUMERIC(26,2)	DEFAULT NULL,
                            operate_receivables_decrease	NUMERIC(26,2)	DEFAULT NULL,
                            operate_payable_increase	NUMERIC(26,2)	DEFAULT NULL,
                            others	NUMERIC(26,2)	DEFAULT NULL,
                            net_operate_cash_flow_indirect	NUMERIC(26,2)	DEFAULT NULL,
                            debt_to_capital	NUMERIC(26,2)	DEFAULT NULL,
                            cbs_expiring_in_one_year	NUMERIC(26,2)	DEFAULT NULL,
                            financial_lease_fixed_assets	NUMERIC(26,2)	DEFAULT NULL,
                            cash_at_end	NUMERIC(26,2)	DEFAULT NULL,
                            cash_at_beginning	NUMERIC(26,2)	DEFAULT NULL,
                            equivalents_at_end	NUMERIC(26,2)	DEFAULT NULL,
                            equivalents_at_beginning	NUMERIC(26,2)	DEFAULT NULL,
                            cash_equivalent_increase_indirect	NUMERIC(26,2)	DEFAULT NULL,
                            net_deposit_increase	NUMERIC(26,2)	DEFAULT NULL,
                            net_borrowing_from_central_bank	NUMERIC(26,2)	DEFAULT NULL,
                            net_borrowing_from_finance_co	NUMERIC(26,2)	DEFAULT NULL,
                            net_original_insurance_cash	NUMERIC(26,2)	DEFAULT NULL,
                            net_cash_received_from_reinsurance_business	NUMERIC(26,2)	DEFAULT NULL,
                            net_insurer_deposit_investment	NUMERIC(26,2)	DEFAULT NULL,
                            net_deal_trading_assets	NUMERIC(26,2)	DEFAULT NULL,
                            interest_and_commission_cashin	NUMERIC(26,2)	DEFAULT NULL,
                            net_increase_in_placements	NUMERIC(26,2)	DEFAULT NULL,
                            net_buyback	NUMERIC(26,2)	DEFAULT NULL,
                            net_loan_and_advance_increase	NUMERIC(26,2)	DEFAULT NULL,
                            net_deposit_in_cb_and_ib	NUMERIC(26,2)	DEFAULT NULL,
                            original_compensation_paid	NUMERIC(26,2)	DEFAULT NULL,
                            handling_charges_and_commission	NUMERIC(26,2)	DEFAULT NULL,
                            policy_dividend_cash_paid	NUMERIC(26,2)	DEFAULT NULL,
                            cash_from_mino_s_invest_sub	NUMERIC(26,2)	DEFAULT NULL,
                            proceeds_from_sub_to_mino_s	NUMERIC(26,2)	DEFAULT NULL,
                            investment_property_depreciation	NUMERIC(26,2)	DEFAULT NULL,
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
                    b.Sname as company_name,
                    b.Symbol as code,
                    b.Exchange,
                    a.PUBLISHDATE as pub_date,
                    a.BEGINDATE as start_date,
                    a.ENDDATE as end_date,
                    a.REPORTDATETYPE as report_date,
                    a.REPORTTYPE as report_type,
                    a.DATASOURCE as source,
                    a.LABORGETCASH as goods_sale_and_service_render_cash,
                    a.TAXREFD as tax_levy_refund,
                    a.BIZCASHINFL as subtotal_operate_cash_inflow,
                    a.LABOPAYC as goods_and_services_cash_paid,
                    a.PAYWORKCASH as staff_behalf_paid,
                    a.PAYTAX as tax_payments,
                    a.BIZCASHOUTF as subtotal_operate_cash_outflow,
                    a.BIZNETCFLOW as net_operate_cash_flow,
                    a.WITHINVGETCASH as invest_withdrawal_cash,
                    a.INVERETUGETCASH as invest_proceeds,
                    a.FIXEDASSETNETC as fix_intan_other_asset_dispo_cash,
                    a.SUBSNETC as net_cash_deal_subcompany,
                    a.INVCASHINFL as subtotal_invest_cash_inflow,
                    a.ACQUASSETCASH as fix_intan_other_asset_acqui_cash,
                    a.INVPAYC as invest_cash_paid,
                    a.LOANNETR as impawned_loan_net_increase,
                    a.SUBSPAYNETCASH as net_cash_from_sub_company,
                    a.INVCASHOUTF as subtotal_invest_cash_outflow,
                    a.INVNETCASHFLOW as net_invest_cash_flow,
                    a.INVRECECASH as cash_from_invest,
                    a.RECEFROMLOAN as cash_from_borrowing,
                    a.ISSBDRECECASH as cash_from_bonds_issue,
                    a.RECEFINCASH as subtotal_finance_cash_inflow,
                    a.FINCASHINFL as borrowing_repayment,
                    a.DEBTPAYCASH as dividend_interest_payment,
                    a.DIVIPROFPAYCASH as subtotal_finance_cash_outflow,
                    a.FINNETCFLOW as net_finance_cash_flow,
                    a.CHGEXCHGCHGS as exchange_rate_change_effect,
                    a.CASHNETI as cash_equivalent_increase,
                    a.EQUOPENBALA as cash_equivalents_at_beginning,
                    a.EQUFINALBALA as cash_and_equivalents_at_end,
                    a.NETPROFIT as net_profit,
                    a.ASSEIMPA as assets_depreciation_reserves,
                    a.ASSEDEPR as fixed_assets_depreciation,
                    a.INTAASSEAMOR as intangible_assets_amortization,
                    a.LONGDEFEEXPENAMOR as defferred_expense_amortization,
                    a.DISPFIXEDASSETLOSS as fix_intan_other_asset_dispo_loss,
                    a.FIXEDASSESCRALOSS as fixed_asset_scrap_loss,
                    a.VALUECHGLOSS as fair_value_change_loss,
                    a.FINEXPE as financial_cost,
                    a.INVELOSS as invest_loss,
                    a.DEFETAXASSETDECR as deffered_tax_asset_decrease,
                    a.DEFETAXLIABINCR as deffered_tax_liability_increase,
                    a.INVEREDU as inventory_decrease,
                    a.RECEREDU as operate_receivables_decrease,
                    a.PAYAINCR as operate_payable_increase,
                    a.OTHER as others,
                    a.BIZNETCFLOW as net_operate_cash_flow_indirect,
                    a.DEBTINTOCAPI as debt_to_capital,
                    a.EXPICONVBD as cbs_expiring_in_one_year,
                    a.FINFIXEDASSET as financial_lease_fixed_assets,
                    a.CASHFINALBALA as cash_at_end,
                    a.CASHOPENBALA as cash_at_beginning,
                    a.EQUFINALBALA as equivalents_at_end,
                    a.EQUOPENBALA as equivalents_at_beginning,
                    a.CASHNETI as cash_equivalent_increase_indirect,
                    a.DEPONETR as net_deposit_increase,
                    a.BANKLOANNETINCR as net_borrowing_from_central_bank,
                    a.FININSTNETR as net_borrowing_from_finance_co,
                    a.INSPREMCASH as net_original_insurance_cash,
                    a.INSNETC as net_cash_received_from_reinsurance_business,
                    a.SAVINETR as net_insurer_deposit_investment,
                    a.DISPTRADNETINCR as net_deal_trading_assets,
                    a.CHARINTECASH as interest_and_commission_cashin,
                    a.FDSBORRNETR as net_increase_in_placements,
                    a.REPNETINCR as net_buyback,
                    a.LOANSNETR as net_loan_and_advance_increase,
                    a.TRADEPAYMNETR as net_deposit_in_cb_and_ib,
                    a.PAYCOMPGOLD as original_compensation_paid,
                    a.PAYINTECASH as handling_charges_and_commission,
                    a.PAYDIVICASH as policy_dividend_cash_paid,
                    a.SUBSRECECASH as cash_from_mino_s_invest_sub,
                    a.SUBSPAYDIVID as proceeds_from_sub_to_mino_s,
                    a.REALESTADEP as investment_property_depreciation,
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
        processor = SyncStkCashflowStatementParent()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkCashflowStatementParent()
        processor.do_update()
    elif args.report:
        processor = SyncStkCashflowStatementParent()
        processor.update_report(args.count, end_date)
