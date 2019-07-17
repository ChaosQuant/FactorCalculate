#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: growth.py
@time: 2019-07-16 19:38
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_growth
from factor.ttm_fundamental import *
from vision.file_unit.balance import Balance
from vision.file_unit.cash_flow import CashFlow
from vision.file_unit.income import Income
from factor.utillities.trade_date import TradeDate
from ultron.cluster.invoke.cache_data import cache_data


def get_trade_date(trade_date, n):
    """
    获取当前时间前n年的时间点，且为交易日，如果非交易日，则往前提取最近的一天。
    :param trade_date: 当前交易日
    :param n:
    :return:
    """
    _trade_date = TradeDate()
    trade_date_sets = collections.OrderedDict(
        sorted(_trade_date._trade_date_sets.items(), key=lambda t: t[0], reverse=False))

    time_array = datetime.strptime(str(trade_date), "%Y%m%d")
    time_array = time_array - timedelta(days=365) * n
    date_time = int(datetime.strftime(time_array, "%Y%m%d"))
    if date_time < min(trade_date_sets.keys()):
        # print('date_time %s is outof trade_date_sets' % date_time)
        return date_time
    else:
        while date_time not in trade_date_sets:
            date_time = date_time - 1
        # print('trade_date pre %s year %s' % (n, date_time))
        return date_time


def get_basic_growth_data(trade_date):
    """
    获取基础数据
    按天获取当天交易日所有股票的基础数据
    :param trade_date: 交易日
    :return:
    """
    trade_date_pre_year = get_trade_date(trade_date, 1)
    trade_date_pre_year_2 = get_trade_date(trade_date, 2)
    trade_date_pre_year_3 = get_trade_date(trade_date, 3)
    trade_date_pre_year_4 = get_trade_date(trade_date, 4)
    trade_date_pre_year_5 = get_trade_date(trade_date, 5)
    # print('trade_date %s' % trade_date)
    # print('trade_date_pre_year %s' % trade_date_pre_year)
    # print('trade_date_pre_year_2 %s' % trade_date_pre_year_2)
    # print('trade_date_pre_year_3 %s' % trade_date_pre_year_3)
    # print('trade_date_pre_year_4 %s' % trade_date_pre_year_4)
    # print('trade_date_pre_year_5 %s' % trade_date_pre_year_5)

    balance_sets = get_fundamentals(add_filter_trade(query(Balance._name_,
                                                           [Balance.symbol,
                                                            Balance.total_assets,  # 总资产（资产合计）
                                                            Balance.total_owner_equities]),  # 股东权益合计
                                                     [trade_date]))

    balance_sets_pre_year = get_fundamentals(add_filter_trade(query(Balance._name_,
                                                                    [Balance.symbol,
                                                                     Balance.total_assets,
                                                                     Balance.total_owner_equities]),
                                                              [trade_date_pre_year]))

    balance_sets_pre_year = balance_sets_pre_year.rename(columns={"total_assets": "total_assets_pre_year",
                                                                  "total_owner_equities": "total_owner_equities_pre_year"})

    balance_sets = pd.merge(balance_sets, balance_sets_pre_year, on='symbol')

    # TTM计算
    ttm_factors = {Income._name_: [Income.symbol,
                                   Income.operating_revenue,  # 营业收入
                                   Income.operating_profit,  # 营业利润
                                   Income.total_profit,  # 利润总额
                                   Income.net_profit,  # 净利润
                                   Income.operating_cost,  # 营业成本
                                   Income.np_parent_company_owners  # 归属于母公司所有者的净利润
                                   ],

                   CashFlow._name_: [CashFlow.symbol,
                                     CashFlow.net_finance_cash_flow,  # 筹资活动产生的现金流量净额
                                     CashFlow.net_operate_cash_flow,  # 经营活动产生的现金流量净额
                                     CashFlow.net_invest_cash_flow,  # 投资活动产生的现金流量净额
                                     ]
                   }

    # TTM计算连续
    ttm_factor_continue = {Income._name_: [Income.symbol,
                                           Income.net_profit,  # 净利润
                                           Income.operating_revenue,  # 营业收入
                                           Income.operating_cost,  # 营业成本
                                           Income.np_parent_company_owners,  # 归属于母公司所有者的净利润
                                           ]
                           }

    ttm_factor_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    ttm_factor_sets = ttm_factor_sets.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year = get_ttm_fundamental([], ttm_factors, trade_date_pre_year).reset_index()
    ttm_factor_sets_pre_year = ttm_factor_sets_pre_year.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year_1 = get_ttm_fundamental([], ttm_factor_continue, trade_date_pre_year).reset_index()
    ttm_factor_sets_pre_year_1 = ttm_factor_sets_pre_year_1.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year_2 = get_ttm_fundamental([], ttm_factor_continue, trade_date_pre_year_2).reset_index()
    ttm_factor_sets_pre_year_2 = ttm_factor_sets_pre_year_2.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year_3 = get_ttm_fundamental([], ttm_factor_continue, trade_date_pre_year_3).reset_index()
    ttm_factor_sets_pre_year_3 = ttm_factor_sets_pre_year_3.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year_4 = get_ttm_fundamental([], ttm_factor_continue, trade_date_pre_year_4).reset_index()
    ttm_factor_sets_pre_year_4 = ttm_factor_sets_pre_year_4.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year_5 = get_ttm_fundamental([], ttm_factor_continue, trade_date_pre_year_5).reset_index()
    ttm_factor_sets_pre_year_5 = ttm_factor_sets_pre_year_5.drop(columns={"trade_date"})

    ttm_factor_sets_pre_year = ttm_factor_sets_pre_year.rename(
        columns={"operating_revenue": "operating_revenue_pre_year",
                 "operating_profit": "operating_profit_pre_year",
                 "total_profit": "total_profit_pre_year",
                 "net_profit": "net_profit_pre_year",
                 "operating_cost": "operating_cost_pre_year",
                 "np_parent_company_owners": "np_parent_company_owners_pre_year",
                 "net_finance_cash_flow": "net_finance_cash_flow_pre_year",
                 "net_operate_cash_flow": "net_operate_cash_flow_pre_year",
                 "net_invest_cash_flow": "net_invest_cash_flow_pre_year"
                 })
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year, on="symbol")

    ttm_factor_sets_pre_year_1 = ttm_factor_sets_pre_year_1.rename(
        columns={"operating_revenue": "operating_revenue_pre_year_1",
                 "operating_cost": "operating_cost_pre_year_1",
                 "net_profit": "net_profit_pre_year_1",
                 "np_parent_company_owners": "np_parent_company_owners_pre_year_1",
                 })
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_1, on="symbol")

    ttm_factor_sets_pre_year_2 = ttm_factor_sets_pre_year_2.rename(
        columns={"operating_revenue": "operating_revenue_pre_year_2",
                 "operating_cost": "operating_cost_pre_year_2",
                 "net_profit": "net_profit_pre_year_2",
                 "np_parent_company_owners": "np_parent_company_owners_pre_year_2",
                 })
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_2, on="symbol")

    ttm_factor_sets_pre_year_3 = ttm_factor_sets_pre_year_3.rename(
        columns={"operating_revenue": "operating_revenue_pre_year_3",
                 "operating_cost": "operating_cost_pre_year_3",
                 "net_profit": "net_profit_pre_year_3",
                 "np_parent_company_owners": "np_parent_company_owners_pre_year_3",
                 })
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_3, on="symbol")

    ttm_factor_sets_pre_year_4 = ttm_factor_sets_pre_year_4.rename(
        columns={"operating_revenue": "operating_revenue_pre_year_4",
                 "operating_cost": "operating_cost_pre_year_4",
                 "net_profit": "net_profit_pre_year_4",
                 "np_parent_company_owners": "np_parent_company_owners_pre_year_4",
                 })
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_4, on="symbol")

    ttm_factor_sets_pre_year_5 = ttm_factor_sets_pre_year_5.rename(
        columns={"operating_revenue": "operating_revenue_pre_year_5",
                 "operating_cost": "operating_cost_pre_year_5",
                 "net_profit": "net_profit_pre_year_5",
                 "np_parent_company_owners": "np_parent_company_owners_pre_year_5",
                 })
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_5, on="symbol")

    return ttm_factor_sets, balance_sets


def prepare_calculate(trade_date):
    # growth
    ttm_factor_sets, balance_sets = get_basic_growth_data(trade_date)
    growth_sets = pd.merge(ttm_factor_sets, balance_sets, on='symbol')
    if len(growth_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date), trade_date, growth_sets.to_json(orient='records'))
        factor_growth.factor_calculate.delay(date_index=trade_date, session=session)
        time1 = time.time()
        print('growth_cal_time:{}'.format(time1 - tic))


def do_update(start_date, end_date, count):
    # 读取本地交易日
    _trade_date = TradeDate()
    trade_date_sets = _trade_date.trade_date_sets_ago(start_date, end_date, count)
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        prepare_calculate(trade_date)
    print('----->')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=1)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)

    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = factor_growth.Growth('factor_growth')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
