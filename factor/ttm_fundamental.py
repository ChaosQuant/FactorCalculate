#!/usr/bin/env python
# coding=utf-8
import pdb
import pandas as pd
from factor.utillities.sync_util import SyncUtil
from vision.fm.signletion_engine import *
from vision.file_unit.balance import Balance
from vision.file_unit.income import Income
from vision.file_unit.cash_flow import CashFlow
key_columns = ['symbol','trade_date','pub_date','report_date']

def get_ttm_fundamental(stock_sets, ttm_factors, date, year = 1):
    sync_util = SyncUtil()
    max_year = int(int(str(date).replace('-','')) / 10000)
    report_date_list = sync_util.ttm_report_date_by_year(date, year)
    # 读取报表信息
    new_fundamental = None
    for key, value in ttm_factors.items():
        ttm_fundamental = None
        value_columns = []
        for v in value:
            if v not in key_columns:
                value_columns.append(v)
        for report_date in report_date_list:
            fundamental_data = get_report(add_filter_trade(query(key,value),[report_date]))
            fundamental_data = fundamental_data[-fundamental_data.duplicated()]
            fundamental_data.set_index('symbol',inplace=True)
            if ttm_fundamental is None:
                ttm_fundamental = fundamental_data
            else:
                ttm_stock_sets = list(set(fundamental_data.index) & set(ttm_fundamental.index))
                ttm_fundamental.loc[ttm_stock_sets,value_columns] += fundamental_data.loc[ttm_stock_sets,value_columns]
                ttm_fundamental = ttm_fundamental.loc[ttm_stock_sets]
        if new_fundamental is None:
            new_fundamental = ttm_fundamental
        else: #
            new_fundamental = pd.merge(new_fundamental, ttm_fundamental, left_index=True, right_index=True)
    new_fundamental['trade_date'] = date
    if len(stock_sets) == 0:
        return new_fundamental
    else:
        return new_fundamental.loc[stock_sets]


if __name__ == '__main__':
    pdb.set_trace()
    stock_sets =  ['600016.XSHG','601229.XSHG','000651.XSHE']
    ttm_factors = {Balance.__name__:[Balance.symbol,Balance.shortterm_loan,Balance.longterm_loan,Balance.total_liability],
                   CashFlow.__name__:[CashFlow.symbol,CashFlow.net_operate_cash_flow,CashFlow.net_finance_cash_flow],
                   Income.__name__:[Income.symbol, Income.total_operating_revenue,Income.total_operating_cost]}

    get_ttm_fundamental(stock_sets, ttm_factors,'2018-10-21')
