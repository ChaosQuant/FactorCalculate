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


class SyncBalance(object):
    def __init__(self):
        self._sync_fun = SyncFundamentals(None, None, 'balance')
        self._sync_fun.secondary_sets(self.secondary_sets)

    def create_dest_tables(self):
        self._sync_fun.create_dest_tables('balance')

    def create_dest_report_tables(self):
        self._sync_fun.create_dest_report_tables('balance_report')

    def create_columns(self):
        columns_list = collections.OrderedDict()
        columns_list['cash_equivalents'] = 'decimal(19,4)'
        columns_list['settlement_provi'] = 'decimal(19,4)'
        columns_list['lend_capital'] = 'decimal(19,4)'
        columns_list['trading_assets'] = 'decimal(19,4)'
        columns_list['bill_receivable'] = 'decimal(19,4)'
        columns_list['account_receivable'] = 'decimal(19,4)'
        columns_list['advance_payment'] = 'decimal(19,4)'
        columns_list['insurance_receivables'] = 'decimal(19,4)'
        columns_list['reinsurance_receivables'] = 'decimal(19,4)'
        columns_list['reinsurance_contract_reserves_receivable'] = 'decimal(19,4)'
        columns_list['interest_receivable'] = 'decimal(19,4)'
        columns_list['dividend_receivable'] = 'decimal(19,4)'
        columns_list['other_receivable'] = 'decimal(19,4)'
        columns_list['bought_sellback_assets'] = 'decimal(19,4)'
        columns_list['inventories'] = 'decimal(19,4)'
        columns_list['non_current_asset_in_one_year'] = 'decimal(19,4)'
        columns_list['other_current_assets'] = 'decimal(19,4)'
        columns_list['total_current_assets'] = 'decimal(19,4)'
        columns_list['loan_and_advance'] = 'decimal(19,4)'
        columns_list['hold_for_sale_assets'] = 'decimal(19,4)'
        columns_list['hold_to_maturity_investments'] = 'decimal(19,4)'
        columns_list['longterm_receivable_account'] = 'decimal(19,4)'
        columns_list['longterm_equity_invest'] = 'decimal(19,4)'
        columns_list['investment_property'] = 'decimal(19,4)'
        columns_list['fixed_assets'] = 'decimal(19,4)'
        columns_list['constru_in_process'] = 'decimal(19,4)'
        columns_list['construction_materials'] = 'decimal(19,4)'
        columns_list['fixed_assets_liquidation'] = 'decimal(19,4)'
        columns_list['biological_assets'] = 'decimal(19,4)'
        columns_list['oil_gas_assets'] = 'decimal(19,4)'
        columns_list['intangible_assets'] = 'decimal(19,4)'
        columns_list['development_expenditure'] = 'decimal(19,4)'
        columns_list['good_will'] = 'decimal(19,4)'
        columns_list['long_deferred_expense'] = 'decimal(19,4)'
        columns_list['deferred_tax_assets'] = 'decimal(19,4)'
        columns_list['other_non_current_assets'] = 'decimal(19,4)'
        columns_list['total_non_current_assets'] = 'decimal(19,4)'
        columns_list['total_assets'] = 'decimal(19,4)'
        columns_list['shortterm_loan'] = 'decimal(19,4)'
        columns_list['borrowing_from_centralbank'] = 'decimal(19,4)'
        columns_list['deposit_in_interbank'] = 'decimal(19,4)'
        columns_list['borrowing_capital'] = 'decimal(19,4)'
        columns_list['trading_liability'] = 'decimal(19,4)'
        columns_list['notes_payable'] = 'decimal(19,4)'
        columns_list['accounts_payable'] = 'decimal(19,4)'
        columns_list['advance_peceipts'] = 'decimal(19,4)'
        columns_list['sold_buyback_secu_proceeds'] = 'decimal(19,4)'
        columns_list['commission_payable'] = 'decimal(19,4)'
        columns_list['salaries_payable'] = 'decimal(19,4)'
        columns_list['taxs_payable'] = 'decimal(19,4)'
        columns_list['interest_payable'] = 'decimal(19,4)'
        columns_list['dividend_payable'] = 'decimal(19,4)'
        columns_list['other_payable'] = 'decimal(19,4)'
        columns_list['reinsurance_payables'] = 'decimal(19,4)'
        columns_list['insurance_contract_reserves'] = 'decimal(19,4)'
        columns_list['proxy_secu_proceeds'] = 'decimal(19,4)'
        columns_list['receivings_from_vicariously_sold_securities'] = 'decimal(19,4)'
        columns_list['non_current_liability_in_one_year'] = 'decimal(19,4)'
        columns_list['other_current_liability'] = 'decimal(19,4)'
        columns_list['total_current_liability'] = 'decimal(19,4)'
        columns_list['longterm_loan'] = 'decimal(19,4)'
        columns_list['bonds_payable'] = 'decimal(19,4)'
        columns_list['longterm_account_payable'] = 'decimal(19,4)'
        columns_list['specific_account_payable'] = 'decimal(19,4)'
        columns_list['estimate_liability'] = 'decimal(19,4)'
        columns_list['deferred_tax_liability'] = 'decimal(19,4)'
        columns_list['other_non_current_liability'] = 'decimal(19,4)'
        columns_list['total_non_current_liability'] = 'decimal(19,4)'
        columns_list['total_liability'] = 'decimal(19,4)'
        columns_list['paidin_capital'] = 'decimal(19,4)'
        columns_list['capital_reserve_fund'] = 'decimal(19,4)'
        columns_list['treasury_stock'] = 'decimal(19,4)'
        columns_list['specific_reserves'] = 'decimal(19,4)'
        columns_list['surplus_reserve_fund'] = 'decimal(19,4)'
        columns_list['ordinary_risk_reserve_fund'] = 'decimal(19,4)'
        columns_list['retained_profit'] = 'decimal(19,4)'
        columns_list['foreign_currency_report_conv_diff'] = 'decimal(19,4)'
        columns_list['equities_parent_company_owners'] = 'decimal(19,4)'
        columns_list['minority_interests'] = 'decimal(19,4)'
        columns_list['total_owner_equities'] = 'decimal(19,4)'
        columns_list['total_sheet_owner_equities'] = 'decimal(19,4)'
        # 二次运算
        columns_list['net_liability'] = 'decimal(19,4)'
        columns_list['interest_bearing_liability'] = 'decimal(19,4)'
        columns_list = collections.OrderedDict(sorted(columns_list.items(), key=lambda t: t[0]))
        time_columns = 'P.ENDDATE'
        del_columns = ['code', 'EXCHANGE', 'SType', 'ReportStyle', 'year']
        sub_columns = []  # 换算单季
        self._sync_fun.set_columns(columns_list, self.create_sql(), time_columns, del_columns,
                                   sub_columns)
        self._sync_fun.set_change_symbol(self.change_symbol)

    def create_sql(self):
        sql = """select S.Symbol AS code,S.Exchange AS EXCHANGE, S.SType, P.PublishDate AS pub_date,
            P.ENDDATE AS report_date,P.REPORTTYPE AS ReportStyle, REPORTYEAR as year,
            P.CURFDS as cash_equivalents,
            P.SETTRESEDEPO as settlement_provi,
            P.PLAC as lend_capital,
            P.TRADFINASSET as trading_assets,
            P.NOTESRECE as bill_receivable,
            P.ACCORECE as account_receivable,
            P.PREP as advance_payment,
            P.PREMRECE as insurance_receivables,
            P.REINRECE as reinsurance_receivables,
            P.REINCONTRESE as reinsurance_contract_reserves_receivable,
            P.INTERECE as interest_receivable,
            P.DIVIDRECE as dividend_receivable,
            P.OTHERRECE as other_receivable,
            P.PURCRESAASSET as bought_sellback_assets,
            P.INVE as inventories,
            P.EXPINONCURRASSET as non_current_asset_in_one_year,
            P.OTHERCURRASSE as other_current_assets,
            P.TOTCURRASSET as total_current_assets,
            P.LENDANDLOAN as loan_and_advance,
            P.AVAISELLASSE as hold_for_sale_assets,
            P.HOLDINVEDUE as hold_to_maturity_investments,
            P.LONGRECE as longterm_receivable_account,
            P.EQUIINVE as longterm_equity_invest,
            P.INVEPROP as investment_property,
            P.FIXEDASSEIMMO as fixed_assets,
            P.CONSPROG as constru_in_process,
            P.ENGIMATE as construction_materials,
            P.FIXEDASSECLEA as fixed_assets_liquidation,
            P.PRODASSE as biological_assets,
            P.HYDRASSET as oil_gas_assets,
            P.INTAASSET as intangible_assets,
            P.DEVEEXPE as development_expenditure,
            P.GOODWILL as good_will,
            P.LOGPREPEXPE as long_deferred_expense,
            P.DEFETAXASSET as deferred_tax_assets,
            P.OTHERNONCASSE as other_non_current_assets,
            P.TOTALNONCASSETS as total_non_current_assets,
            P.TOTASSET as total_assets,
            P.SHORTTERMBORR as shortterm_loan,
            P.CENBANKBORR as borrowing_from_centralbank,
            P.DEPOSIT as deposit_in_interbank,
            P.FDSBORR as borrowing_capital,
            P.TRADFINLIAB as trading_liability,
            P.NOTESPAYA as notes_payable,
            P.ACCOPAYA as accounts_payable,
            P.ADVAPAYM as advance_peceipts,
            P.SELLREPASSE as sold_buyback_secu_proceeds,
            P.COPEPOUN as commission_payable,
            P.COPEWORKERSAL as salaries_payable,
            P.TAXESPAYA as taxs_payable,
            P.INTEPAYA as interest_payable,
            P.DIVIPAYA as dividend_payable,
            P.OTHERFEEPAYA as other_payable,
            P.COPEWITHREINRECE as reinsurance_payables,
            P.INSUCONTRESE as insurance_contract_reserves,
            P.ACTITRADSECU as proxy_secu_proceeds,
            P.ACTIUNDESECU as receivings_from_vicariously_sold_securities,
            P.DUENONCLIAB as non_current_liability_in_one_year,
            P.OTHERCURRELIABI as other_current_liability,
            P.TOTALCURRLIAB as total_current_liability,
            P.LONGBORR as longterm_loan,
            P.BDSPAYA as bonds_payable,
            P.LONGPAYA as longterm_account_payable,
            P.SPECPAYA as specific_account_payable,
            P.EXPENONCLIAB as estimate_liability,
            P.DEFEINCOTAXLIAB as deferred_tax_liability,
            P.OTHERNONCLIABI as other_non_current_liability,
            P.TOTALNONCLIAB as total_non_current_liability,
            P.TOTLIAB as total_liability,
            P.PAIDINCAPI as paidin_capital,
            P.CAPISURP as capital_reserve_fund,
            P.TREASTK as treasury_stock,
            P.SPECRESE as specific_reserves,
            P.RESE as surplus_reserve_fund,
            P.GENERISKRESE as ordinary_risk_reserve_fund,
            P.UNDIPROF as retained_profit,
            P.CURTRANDIFF as foreign_currency_report_conv_diff,
            P.PARESHARRIGH as equities_parent_company_owners,
            P.MINYSHARRIGH as minority_interests,
            P.RIGHAGGR as total_owner_equities,
            P.TOTLIABSHAREQUI as total_sheet_owner_equities
            from QADB.dbo.TQ_FIN_PROBALSHEETNEW AS P JOIN FCDB.dbo.SecurityCode as S ON
            S.CompanyCode = P.COMPCODE
            where P.REPORTTYPE={0} and S.SType='{1}' and S.Enabled=0 and S.Status=0 and """.format(1,
                                                                                                   'EQA')
        return sql

    def secondary_sets(self, trades_date_fundamentals):
        # 净资产
        trades_date_fundamentals['net_liability'] = trades_date_fundamentals['shortterm_loan'] + \
                                                    trades_date_fundamentals['longterm_loan'] + \
                                                    trades_date_fundamentals['bonds_payable']
        # 有息负债
        trades_date_fundamentals['interest_bearing_liability'] = trades_date_fundamentals['net_liability'] - \
                                                                 trades_date_fundamentals[
                                                                     'non_current_liability_in_one_year']
        # 资产合计
        # trades_date_fundamentals['total_assets'] = trades_date_fundamentals['total_current_assets'] + \
        #                                trades_date_fundamentals['total_current_assets']
        return trades_date_fundamentals

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
        processor = SyncBalance()
        processor.create_columns()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.update:
        processor = SyncBalance()
        processor.create_columns()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.rebuild_report:
        processor = SyncBalance()
        processor.create_columns()
        processor.create_dest_report_tables()
        processor.update_report(args.start_date, end_date, args.count)
    elif args.report:
        processor = SyncBalance()
        processor.create_columns()
        processor.update_report(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncBalance()
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name, 'trade_date')
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name + '_report', 'report_date')
        print('running schedule report task, start date:', start_date, ';end date:', end_date)
        processor.update_report(start_date, end_date, -1)
