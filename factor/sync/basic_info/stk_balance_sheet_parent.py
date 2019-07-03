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


class SyncStkBalanceSheetParent(BaseSync):
    def __init__(self, source=None, destination=None):
        self.source_table = 'TQ_FIN_PROBALSHEETNEW'
        self.dest_table = 'stk_balance_sheet_parent'
        super(SyncStkBalanceSheetParent, self).__init__(self.dest_table)
        self.utils = TmImportUtils(self.source, self.destination, self.source_table, self.dest_table)

    # 创建目标表
    def create_dest_tables(self):
        self.utils.update_update_log(0)
        create_sql = """create table {0}
                        (
                            id	VARCHAR(12)	NOT NULL,
                            company_id	VARCHAR(20)	NOT NULL,
                            company_name	VARCHAR(100)	CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'	NOT NULL,
                            symbol	VARCHAR(12)	NOT NULL,
                            pub_date	Date	NOT NULL,
                            end_date	Date	NOT NULL,
                            report_type	VARCHAR(10)	NOT NULL,
                            report_date	VARCHAR(2)	NOT NULL,
                            source	VARCHAR(10)	NOT NULL,
                            cash_equivalents	NUMERIC(26,2)	DEFAULT NULL,
                            trading_assets	NUMERIC(26,2)	DEFAULT NULL,
                            bill_receivable	NUMERIC(26,2)	DEFAULT NULL,
                            account_receivable	NUMERIC(26,2)	DEFAULT NULL,
                            advance_payment	NUMERIC(26,2)	DEFAULT NULL,
                            other_receivable	NUMERIC(26,2)	DEFAULT NULL,
                            interest_receivable	NUMERIC(26,2)	DEFAULT NULL,
                            dividend_receivable	NUMERIC(26,2)	DEFAULT NULL,
                            inventories	NUMERIC(26,2)	DEFAULT NULL,
                            non_current_asset_in_one_year	NUMERIC(26,2)	DEFAULT NULL,
                            total_current_assets	NUMERIC(26,2)	DEFAULT NULL,
                            hold_for_sale_assets	NUMERIC(26,2)	DEFAULT NULL,
                            hold_to_maturity_investments	NUMERIC(26,2)	DEFAULT NULL,
                            longterm_receivable_account	NUMERIC(26,2)	DEFAULT NULL,
                            longterm_equity_invest	NUMERIC(26,2)	DEFAULT NULL,
                            investment_property	NUMERIC(26,2)	DEFAULT NULL,
                            fixed_assets	NUMERIC(26,2)	DEFAULT NULL,
                            constru_in_process	NUMERIC(26,2)	DEFAULT NULL,
                            construction_materials	NUMERIC(26,2)	DEFAULT NULL,
                            fixed_assets_liquidation	NUMERIC(26,2)	DEFAULT NULL,
                            biological_assets	NUMERIC(26,2)	DEFAULT NULL,
                            oil_gas_assets	NUMERIC(26,2)	DEFAULT NULL,
                            intangible_assets	NUMERIC(26,2)	DEFAULT NULL,
                            development_expenditure	NUMERIC(26,2)	DEFAULT NULL,
                            good_will	NUMERIC(26,2)	DEFAULT NULL,
                            long_deferred_expense	NUMERIC(26,2)	DEFAULT NULL,
                            deferred_tax_assets	NUMERIC(26,2)	DEFAULT NULL,
                            total_non_current_assets	NUMERIC(26,2)	DEFAULT NULL,
                            total_assets	NUMERIC(26,2)	DEFAULT NULL,
                            shortterm_loan	NUMERIC(26,2)	DEFAULT NULL,
                            trading_liability	NUMERIC(26,2)	DEFAULT NULL,
                            notes_payable	NUMERIC(26,2)	DEFAULT NULL,
                            accounts_payable	NUMERIC(26,2)	DEFAULT NULL,
                            advance_peceipts	NUMERIC(26,2)	DEFAULT NULL,
                            salaries_payable	NUMERIC(26,2)	DEFAULT NULL,
                            taxs_payable	NUMERIC(26,2)	DEFAULT NULL,
                            interest_payable	NUMERIC(26,2)	DEFAULT NULL,
                            dividend_payable	NUMERIC(26,2)	DEFAULT NULL,
                            other_payable	NUMERIC(26,2)	DEFAULT NULL,
                            non_current_liability_in_one_year	NUMERIC(26,2)	DEFAULT NULL,
                            total_current_liability	NUMERIC(26,2)	DEFAULT NULL,
                            longterm_loan	NUMERIC(26,2)	DEFAULT NULL,
                            bonds_payable	NUMERIC(26,2)	DEFAULT NULL,
                            longterm_account_payable	NUMERIC(26,2)	DEFAULT NULL,
                            specific_account_payable	NUMERIC(26,2)	DEFAULT NULL,
                            estimate_liability	NUMERIC(26,2)	DEFAULT NULL,
                            deferred_tax_liability	NUMERIC(26,2)	DEFAULT NULL,
                            total_non_current_liability	NUMERIC(26,2)	DEFAULT NULL,
                            total_liability	NUMERIC(26,2)	DEFAULT NULL,
                            paidin_capital	NUMERIC(26,2)	DEFAULT NULL,
                            capital_reserve_fund	NUMERIC(26,2)	DEFAULT NULL,
                            specific_reserves	NUMERIC(26,2)	DEFAULT NULL,
                            surplus_reserve_fund	NUMERIC(26,2)	DEFAULT NULL,
                            treasury_stock	NUMERIC(26,2)	DEFAULT NULL,
                            retained_profit	NUMERIC(26,2)	DEFAULT NULL,
                            equities_parent_company_owners	NUMERIC(26,2)	DEFAULT NULL,
                            minority_interests	NUMERIC(26,2)	DEFAULT NULL,
                            foreign_currency_report_conv_diff	NUMERIC(26,2)	DEFAULT NULL,
                            total_owner_equities	NUMERIC(26,2)	DEFAULT NULL,
                            total_sheet_owner_equities	NUMERIC(26,2)	DEFAULT NULL,
                            other_comprehesive_income	NUMERIC(26,2)	DEFAULT NULL,
                            deferred_earning	NUMERIC(26,2)	DEFAULT NULL,
                            settlement_provi	NUMERIC(26,2)	DEFAULT NULL,
                            lend_capital	NUMERIC(26,2)	DEFAULT NULL,
                            loan_and_advance_current_assets	NUMERIC(26,2)	DEFAULT NULL,
                            insurance_receivables	NUMERIC(26,2)	DEFAULT NULL,
                            reinsurance_receivables	NUMERIC(26,2)	DEFAULT NULL,
                            reinsurance_contract_reserves_receivable	NUMERIC(26,2)	DEFAULT NULL,
                            bought_sellback_assets	NUMERIC(26,2)	DEFAULT NULL,
                            hold_sale_asset	NUMERIC(26,2)	DEFAULT NULL,
                            loan_and_advance_noncurrent_assets	NUMERIC(26,2)	DEFAULT NULL,
                            borrowing_from_centralbank	NUMERIC(26,2)	DEFAULT NULL,
                            deposit_in_interbank	NUMERIC(26,2)	DEFAULT NULL,
                            borrowing_capital	NUMERIC(26,2)	DEFAULT NULL,
                            derivative_financial_liability	NUMERIC(26,2)	DEFAULT NULL,
                            sold_buyback_secu_proceeds	NUMERIC(26,2)	DEFAULT NULL,
                            commission_payable	NUMERIC(26,2)	DEFAULT NULL,
                            reinsurance_payables	NUMERIC(26,2)	DEFAULT NULL,
                            insurance_contract_reserves	NUMERIC(26,2)	DEFAULT NULL,
                            proxy_secu_proceeds	NUMERIC(26,2)	DEFAULT NULL,
                            receivings_from_vicariously_sold_securities	NUMERIC(26,2)	DEFAULT NULL,
                            hold_sale_liability	NUMERIC(26,2)	DEFAULT NULL,
                            estimate_liability_current	NUMERIC(26,2)	DEFAULT NULL,
                            preferred_shares_noncurrent	NUMERIC(26,2)	DEFAULT NULL,
                            pepertual_liability_noncurrent	NUMERIC(26,2)	DEFAULT NULL,
                            longterm_salaries_payable	NUMERIC(26,2)	DEFAULT NULL,
                            other_equity_tools	NUMERIC(26,2)	DEFAULT NULL,
                            tmstamp        bigint       not null,
                            PRIMARY KEY(`symbol`,`end_date`,`report_type`)
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
                    a.PUBLISHDATE as pub_date,
                    a.ENDDATE as end_date,
                    a.REPORTDATETYPE as report_date,
                    a.REPORTTYPE as report_type,
                    a.DATASOURCE as source,
                    a.CURFDS as cash_equivalents,
                    a.TRADFINASSET as trading_assets,
                    a.NOTESRECE as bill_receivable,
                    a.ACCORECE as account_receivable,
                    a.PREP as advance_payment,
                    a.OTHERRECE as other_receivable,
                    a.INTERECE as interest_receivable,
                    a.DIVIDRECE as dividend_receivable,
                    a.INVE as inventories,
                    a.EXPINONCURRASSET as non_current_asset_in_one_year,
                    a.TOTCURRASSET as total_current_assets,
                    a.AVAISELLASSE as hold_for_sale_assets,
                    a.HOLDINVEDUE as hold_to_maturity_investments,
                    a.LONGRECE as longterm_receivable_account,
                    a.EQUIINVE as longterm_equity_invest,
                    a.INVEPROP as investment_property,
                    a.FIXEDASSEIMMO as fixed_assets,
                    a.CONSPROG as constru_in_process,
                    a.ENGIMATE as construction_materials,
                    a.FIXEDASSECLEA as fixed_assets_liquidation,
                    a.PRODASSE as biological_assets,
                    a.HYDRASSET as oil_gas_assets,
                    a.INTAASSET as intangible_assets,
                    a.DEVEEXPE as development_expenditure,
                    a.GOODWILL as good_will,
                    a.LOGPREPEXPE as long_deferred_expense,
                    a.DEFETAXASSET as deferred_tax_assets,
                    a.TOTALNONCASSETS as total_non_current_assets,
                    a.TOTASSET as total_assets,
                    a.SHORTTERMBORR as shortterm_loan,
                    a.TRADFINLIAB as trading_liability,
                    a.NOTESPAYA as notes_payable,
                    a.ACCOPAYA as accounts_payable,
                    a.ADVAPAYM as advance_peceipts,
                    a.COPEWORKERSAL as salaries_payable,
                    a.TAXESPAYA as taxs_payable,
                    a.INTEPAYA as interest_payable,
                    a.DIVIPAYA as dividend_payable,
                    a.OTHERFEEPAYA as other_payable,
                    a.DUENONCLIAB as non_current_liability_in_one_year,
                    a.TOTALCURRLIAB as total_current_liability,
                    a.LONGBORR as longterm_loan,
                    a.BDSPAYA as bonds_payable,
                    a.LONGPAYA as longterm_account_payable,
                    a.SPECPAYA as specific_account_payable,
                    a.EXPECURRLIAB+EXPENONCLIAB as estimate_liability,
                    a.DEFEINCOTAXLIAB as deferred_tax_liability,
                    a.TOTALNONCLIAB as total_non_current_liability,
                    a.TOTLIAB as total_liability,
                    a.PAIDINCAPI as paidin_capital,
                    a.CAPISURP as capital_reserve_fund,
                    a.SPECRESE as specific_reserves,
                    a.RESE as surplus_reserve_fund,
                    a.TREASTK as treasury_stock,
                    a.UNDIPROF as retained_profit,
                    a.PARESHARRIGH as equities_parent_company_owners,
                    a.MINYSHARRIGH as minority_interests,
                    a.CURTRANDIFF as foreign_currency_report_conv_diff,
                    a.RIGHAGGR as total_owner_equities,
                    a.TOTLIABSHAREQUI as total_sheet_owner_equities,
                    a.OCL as other_comprehesive_income,
                    a.DEFEREVE as deferred_earning,
                    a.SETTRESEDEPO as settlement_provi,
                    a.PLAC as lend_capital,
                    a.LENDANDLOAN as loan_and_advance_current_assets,
                    a.PREMRECE as insurance_receivables,
                    a.REINRECE as reinsurance_receivables,
                    a.REINCONTRESE as reinsurance_contract_reserves_receivable,
                    a.PURCRESAASSET as bought_sellback_assets,
                    a.ACCHELDFORS as hold_sale_asset,
                    a.LENDANDLOAN as loan_and_advance_noncurrent_assets,
                    a.CENBANKBORR as borrowing_from_centralbank,
                    a.DEPOSIT as deposit_in_interbank,
                    a.FDSBORR as borrowing_capital,
                    a.DERILIAB as derivative_financial_liability,
                    a.SELLREPASSE as sold_buyback_secu_proceeds,
                    a.COPEPOUN as commission_payable,
                    a.COPEWITHREINRECE as reinsurance_payables,
                    a.INSUCONTRESE as insurance_contract_reserves,
                    a.ACTITRADSECU as proxy_secu_proceeds,
                    a.ACTIUNDESECU as receivings_from_vicariously_sold_securities,
                    a.LIABHELDFORS as hold_sale_liability,
                    a.EXPECURRLIAB as estimate_liability_current,
                    a.PREST as preferred_shares_noncurrent,
                    a.PERBOND as pepertual_liability_noncurrent,
                    a.LCOPEWORKERSAL as longterm_salaries_payable,
                    a.OTHEQUIN as other_equity_tools,
                    b.Exchange,
                    cast(a.tmstamp as bigint) as tmstamp
                    from {1} a 
                    left join FCDB.dbo.SecurityCode as b
                    on b.CompanyCode = a.COMPCODE
                    where REPORTTYPE in ('2','4')  and b.SType ='EQA' and b.Enabled=0 and b.status=0  and a.ACCSTACODE=11002"""
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
        processor = SyncStkBalanceSheetParent()
        processor.create_dest_tables()
        processor.do_update()
    elif args.update:
        processor = SyncStkBalanceSheetParent()
        processor.do_update()
    elif args.report:
        processor = SyncStkBalanceSheetParent()
        processor.update_report(args.count, end_date)
