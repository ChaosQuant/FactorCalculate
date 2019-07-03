#!/usr/bin/env python
# coding=utf-8
import pdb
import collections
import numpy as np
from datetime import datetime
import sqlalchemy as sa
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import argparse
from sync_fundamentals import SyncFundamentals


class SyncCashFlow(object):
    def __init__(self):
        self._sync_fun = SyncFundamentals(sa.create_engine("mssql+pymssql://read:read@192.168.100.64:1433/QADB"),
                                          None, 'cash_flow')

    def create_dest_tables(self):
        self._sync_fun.create_dest_tables('cash_flow')

    def create_dest_report_tables(self):
        self._sync_fun.create_dest_report_tables('cash_flow_report')

    def create_columns(self):
        columns_list = collections.OrderedDict()
        columns_list['goods_sale_and_service_render_cash'] = 'decimal(19,4)'
        columns_list['net_deposit_increase'] = 'decimal(19,4)'
        columns_list['net_borrowing_from_central_bank'] = 'decimal(19,4)'
        columns_list['net_borrowing_from_finance_co'] = 'decimal(19,4)'
        columns_list['net_original_insurance_cash'] = 'decimal(19,4)'
        columns_list['net_cash_received_from_reinsurance_business'] = 'decimal(19,4)'
        columns_list['net_insurer_deposit_investment'] = 'decimal(19,4)'
        columns_list['net_deal_trading_assets'] = 'decimal(19,4)'
        columns_list['interest_and_commission_cashin'] = 'decimal(19,4)'
        columns_list['net_increase_in_placements'] = 'decimal(19,4)'
        columns_list['net_buyback'] = 'decimal(19,4)'
        columns_list['tax_levy_refund'] = 'decimal(19,4)'
        columns_list['other_cashin_related_operate'] = 'decimal(19,4)'
        columns_list['subtotal_operate_cash_inflow'] = 'decimal(19,4)'
        columns_list['goods_and_services_cash_paid'] = 'decimal(19,4)'
        columns_list['net_loan_and_advance_increase'] = 'decimal(19,4)'
        columns_list['net_deposit_in_cb_and_ib'] = 'decimal(19,4)'
        columns_list['original_compensation_paid'] = 'decimal(19,4)'
        columns_list['handling_charges_and_commission'] = 'decimal(19,4)'
        columns_list['policy_dividend_cash_paid'] = 'decimal(19,4)'
        columns_list['staff_behalf_paid'] = 'decimal(19,4)'
        columns_list['tax_payments'] = 'decimal(19,4)'
        columns_list['other_operate_cash_paid'] = 'decimal(19,4)'
        columns_list['subtotal_operate_cash_outflow'] = 'decimal(19,4)'
        columns_list['net_operate_cash_flow'] = 'decimal(19,4)'
        columns_list['invest_withdrawal_cash'] = 'decimal(19,4)'
        columns_list['invest_proceeds'] = 'decimal(19,4)'
        columns_list['fix_intan_other_asset_dispo_cash'] = 'decimal(19,4)'
        columns_list['net_cash_deal_subcompany'] = 'decimal(19,4)'
        columns_list['other_cash_from_invest_act'] = 'decimal(19,4)'
        columns_list['subtotal_invest_cash_inflow'] = 'decimal(19,4)'
        columns_list['fix_intan_other_asset_acqui_cash'] = 'decimal(19,4)'
        columns_list['invest_cash_paid'] = 'decimal(19,4)'
        columns_list['impawned_loan_net_increase'] = 'decimal(19,4)'
        columns_list['net_cash_from_sub_company'] = 'decimal(19,4)'
        columns_list['other_cash_to_invest_act'] = 'decimal(19,4)'
        columns_list['subtotal_invest_cash_outflow'] = 'decimal(19,4)'
        columns_list['net_invest_cash_flow'] = 'decimal(19,4)'
        columns_list['cash_from_invest'] = 'decimal(19,4)'
        columns_list['cash_from_mino_s_invest_sub'] = 'decimal(19,4)'
        columns_list['cash_from_borrowing'] = 'decimal(19,4)'
        columns_list['cash_from_bonds_issue'] = 'decimal(19,4)'
        columns_list['other_finance_act_cash'] = 'decimal(19,4)'
        columns_list['subtotal_finance_cash_inflow'] = 'decimal(19,4)'
        columns_list['borrowing_repayment'] = 'decimal(19,4)'
        columns_list['dividend_interest_payment'] = 'decimal(19,4)'
        columns_list['proceeds_from_sub_to_mino_s'] = 'decimal(19,4)'
        columns_list['other_finance_act_payment'] = 'decimal(19,4)'
        columns_list['subtotal_finance_cash_outflow'] = 'decimal(19,4)'
        columns_list['net_finance_cash_flow'] = 'decimal(19,4)'
        columns_list['exchange_rate_change_effect'] = 'decimal(19,4)'
        columns_list['cash_equivalent_increase'] = 'decimal(19,4)'
        columns_list['cash_equivalents_at_beginning'] = 'decimal(19,4)'
        columns_list['cash_and_equivalents_at_end'] = 'decimal(19,4)'

        columns_list = collections.OrderedDict(sorted(columns_list.items(), key=lambda t: t[0]))
        time_columns = 'P.ENDDATE'
        del_columns = ['code', 'EXCHANGE', 'SType', 'ReportStyle', 'year']
        sub_columns = ['goods_sale_and_service_render_cash', 'net_deposit_increase', 'net_borrowing_from_central_bank',
                       'net_borrowing_from_finance_co', 'net_original_insurance_cash',
                       'net_cash_received_from_reinsurance_business', 'net_insurer_deposit_investment',
                       'net_deal_trading_assets', 'interest_and_commission_cashin', 'net_increase_in_placements',
                       'net_buyback', 'tax_levy_refund', 'other_cashin_related_operate', 'subtotal_operate_cash_inflow',
                       'goods_and_services_cash_paid', 'net_loan_and_advance_increase', 'net_deposit_in_cb_and_ib',
                       'original_compensation_paid', 'handling_charges_and_commission', 'policy_dividend_cash_paid',
                       'staff_behalf_paid', 'tax_payments', 'other_operate_cash_paid', 'subtotal_operate_cash_outflow',
                       'net_operate_cash_flow', 'invest_withdrawal_cash', 'invest_proceeds',
                       'fix_intan_other_asset_dispo_cash', 'net_cash_deal_subcompany', 'other_cash_from_invest_act',
                       'subtotal_invest_cash_inflow', 'fix_intan_other_asset_acqui_cash', 'invest_cash_paid',
                       'impawned_loan_net_increase', 'net_cash_from_sub_company', 'other_cash_to_invest_act',
                       'subtotal_invest_cash_outflow', 'net_invest_cash_flow', 'cash_from_invest',
                       'cash_from_mino_s_invest_sub', 'cash_from_borrowing', 'cash_from_bonds_issue',
                       'other_finance_act_cash', 'subtotal_finance_cash_inflow', 'borrowing_repayment',
                       'dividend_interest_payment', 'proceeds_from_sub_to_mino_s', 'other_finance_act_payment',
                       'subtotal_finance_cash_outflow', 'net_finance_cash_flow', 'exchange_rate_change_effect',
                       'cash_equivalent_increase', 'cash_equivalents_at_beginning',
                       'cash_and_equivalents_at_end']  # 换算单季
        self._sync_fun.set_columns(columns_list, self.create_sql(), time_columns, del_columns,
                                   sub_columns)
        self._sync_fun.set_change_symbol(self.change_symbol)

    def create_sql(self):
        sql = """select S.Symbol AS code,S.Exchange AS EXCHANGE, S.SType, P.PublishDate AS pub_date,
                    P.ENDDATE AS report_date,P.REPORTTYPE AS ReportStyle, REPORTYEAR as year,
                    P.LABORGETCASH as goods_sale_and_service_render_cash,
                    P.DEPONETR as net_deposit_increase,
                    P.BANKLOANNETINCR as net_borrowing_from_central_bank,
                    P.FININSTNETR as net_borrowing_from_finance_co,
                    P.INSPREMCASH as net_original_insurance_cash,
                    P.INSNETC as net_cash_received_from_reinsurance_business,
                    P.SAVINETR as net_insurer_deposit_investment,
                    P.DISPTRADNETINCR as net_deal_trading_assets,
                    P.CHARINTECASH as interest_and_commission_cashin,
                    P.FDSBORRNETR as net_increase_in_placements,
                    P.REPNETINCR as net_buyback,
                    P.TAXREFD as tax_levy_refund,
                    P.RECEOTHERBIZCASH as other_cashin_related_operate,
                    P.BIZCASHINFL as subtotal_operate_cash_inflow,
                    P.LABOPAYC as goods_and_services_cash_paid,
                    P.LOANSNETR as net_loan_and_advance_increase,
                    P.TRADEPAYMNETR as net_deposit_in_cb_and_ib,
                    P.PAYCOMPGOLD as original_compensation_paid,
                    P.PAYINTECASH as handling_charges_and_commission,
                    P.PAYDIVICASH as policy_dividend_cash_paid,
                    P.PAYWORKCASH as staff_behalf_paid,
                    P.PAYTAX as tax_payments,
                    P.PAYACTICASH as other_operate_cash_paid,
                    P.BIZCASHOUTF as subtotal_operate_cash_outflow,
                    P.MANANETR as net_operate_cash_flow,
                    P.WITHINVGETCASH as invest_withdrawal_cash,
                    P.INVERETUGETCASH as invest_proceeds,
                    P.FIXEDASSETNETC as fix_intan_other_asset_dispo_cash,
                    P.SUBSNETC as net_cash_deal_subcompany,
                    P.RECEINVCASH as other_cash_from_invest_act,
                    P.INVCASHINFL as subtotal_invest_cash_inflow,
                    P.ACQUASSETCASH as fix_intan_other_asset_acqui_cash,
                    P.PAYINVECASH as invest_cash_paid,
                    P.LOANNETR as impawned_loan_net_increase,
                    P.SUBSPAYNETCASH as net_cash_from_sub_company,
                    P.PAYINVECASH as other_cash_to_invest_act,
                    P.INVCASHOUTF as subtotal_invest_cash_outflow,
                    P.INVNETCASHFLOW as net_invest_cash_flow,
                    P.INVRECECASH as cash_from_invest,
                    P.SUBSRECECASH as cash_from_mino_s_invest_sub,
                    P.RECEFROMLOAN as cash_from_borrowing,
                    P.ISSBDRECECASH as cash_from_bonds_issue,
                    P.RECEFINCASH as other_finance_act_cash,
                    P.FINCASHINFL as subtotal_finance_cash_inflow,
                    P.DEBTPAYCASH as borrowing_repayment,
                    P.DIVIPROFPAYCASH as dividend_interest_payment,
                    P.SUBSPAYDIVID as proceeds_from_sub_to_mino_s,
                    P.FINRELACASH as other_finance_act_payment,
                    P.FINCASHOUTF as subtotal_finance_cash_outflow,
                    P.FINNETCFLOW as net_finance_cash_flow,
                    P.CHGEXCHGCHGS as exchange_rate_change_effect,
                    P.CASHNETR as cash_equivalent_increase,
                    P.INICASHBALA as cash_equivalents_at_beginning,
                    FINALCASHBALA as cash_and_equivalents_at_end
                    from QADB.dbo.TQ_FIN_PROCFSTATEMENTNEW   AS P JOIN FCDB.dbo.SecurityCode as S ON
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
        processor = SyncCashFlow()
        processor.create_columns()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.update:
        processor = SyncCashFlow()
        processor.create_columns()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.rebuild_report:
        processor = SyncCashFlow()
        processor.create_columns()
        processor.create_dest_report_tables()
        processor.update_report(args.start_date, end_date, args.count)
    elif args.report:
        processor = SyncCashFlow()
        processor.create_columns()
        processor.update_report(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncCashFlow()
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name, 'trade_date')
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name + '_report', 'report_date')
        print('running schedule report task, start date:', start_date, ';end date:', end_date)
        processor.update_report(start_date, end_date, -1)