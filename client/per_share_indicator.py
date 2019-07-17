#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: per_share_indicator.py
@time: 2019-07-16 19:51
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_per_share_indicators
from factor.ttm_fundamental import *
from vision.file_unit.balance import Balance
from vision.file_unit.cash_flow import CashFlow
from vision.file_unit.income import Income
from vision.file_unit.valuation import Valuation
from vision.file_unit.industry import Industry
from vision.file_unit.indicator import Indicator
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


def get_basic_scale_data(trade_date):
    """
    获取基础数据
    按天获取当天交易日所有股票的基础数据
    :param trade_date: 交易日
    :return:
    """
    valuation_sets = get_fundamentals(add_filter_trade(query(Valuation._name_,
                                                             [Valuation.symbol,
                                                              Valuation.market_cap,
                                                              Valuation.capitalization,  # 总股本
                                                              Valuation.circulating_market_cap]),  #
                                                       [trade_date]))

    cash_flow_sets = get_fundamentals(add_filter_trade(query(CashFlow._name_,
                                                             [CashFlow.symbol,
                                                              CashFlow.cash_and_equivalents_at_end,  # 现金及现金等价物净增加额
                                                              CashFlow.cash_equivalent_increase]),  # 期末现金及现金等价物余额(元)
                                                       [trade_date]))

    income_sets = get_fundamentals(add_filter_trade(query(Income._name_,
                                                          [Income.symbol,
                                                           Income.basic_eps,  # 基本每股收益
                                                           Income.diluted_eps,   # 稀释每股收益
                                                           Income.net_profit,
                                                           Income.operating_revenue,  # 营业收入
                                                           Income.operating_profit,  # 营业利润
                                                           Income.total_operating_revenue]),  # 营业总收入
                                                    [trade_date]))

    balance_sets = get_fundamentals(add_filter_trade(query(Balance._name_,
                                                           [Balance.symbol,
                                                            Balance.capital_reserve_fund,  # 资本公积
                                                            Balance.surplus_reserve_fund,  # 盈余公积
                                                            Balance.total_assets,  # 总资产（资产合计)
                                                            Balance.dividend_receivable,  # 股利
                                                            Balance.retained_profit,  # 未分配利润
                                                            Balance.total_owner_equities]),  # 归属于母公司的所有者权益
                                                     [trade_date]))

    # TTM计算
    ttm_factors = {Income._name_: [Income.symbol,
                                   Income.operating_revenue,  # 营业收入
                                   Income.operating_profit,  # 营业利润
                                   Income.np_parent_company_owners,  # 归属于母公司所有者股东的净利润
                                   Income.total_operating_revenue],  # 营业总收入

                   CashFlow._name_: [CashFlow.symbol,
                                     CashFlow.net_operate_cash_flow]  # 经营活动产生的现金流量净额
                   }

    ttm_factor_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()

    ttm_factor_sets = ttm_factor_sets.rename(columns={"np_parent_company_owners": "np_parent_company_owners_ttm"})
    ttm_factor_sets = ttm_factor_sets.rename(columns={"net_operate_cash_flow": "net_operate_cash_flow_ttm"})
    ttm_factor_sets = ttm_factor_sets.rename(columns={"operating_revenue": "operating_revenue_ttm"})
    ttm_factor_sets = ttm_factor_sets.rename(columns={"operating_profit": "operating_profit_ttm"})
    ttm_factor_sets = ttm_factor_sets.rename(columns={"total_operating_revenue": "total_operating_revenue_ttm"})
    ttm_factor_sets = ttm_factor_sets.drop(columns={"trade_date"})

    return valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets, balance_sets


def prepare_calculate(trade_date):
    # per share indicators
    valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets, balance_sets = get_basic_scale_data(trade_date)
    valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, balance_sets, on='symbol')
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date), trade_date, valuation_sets.to_json(orient='records'))
        factor_per_share_indicators.factor_calculate.delay(date_index=trade_date, session=session)
        time3 = time.time()
        print('per_share_cal_time:{}'.format(time3 - tic))


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
        processor = factor_per_share_indicators.PerShareIndicators('factor_per_share')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
