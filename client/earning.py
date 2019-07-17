#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: earning.py
@time: 2019-07-16 19:44
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_earning
from factor.ttm_fundamental import *
from vision.file_unit.balance import Balance
from vision.file_unit.cash_flow import CashFlow
from vision.file_unit.income import Income
from vision.file_unit.valuation import Valuation
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


def get_basic_earning(trade_date):
    # 读取目前涉及到的因子
    # 当期数据
    # pdb.set_trace()
    balance_sets = get_fundamentals(add_filter_trade(query(Balance.__name__,
                                                           [Balance.symbol,
                                                            Balance.equities_parent_company_owners])
                                                     , [trade_date]))
    cash_flow_sets = get_fundamentals(add_filter_trade(query(CashFlow.__name__,
                                                             [CashFlow.symbol,
                                                              CashFlow.goods_sale_and_service_render_cash])
                                                       , [trade_date]))
    income_sets = get_fundamentals(add_filter_trade(query(Income.__name__,
                                                          [Income.symbol,
                                                           Income.total_operating_revenue,
                                                           Income.total_operating_cost,
                                                           Income.invest_income_associates,
                                                           Income.non_operating_revenue,
                                                           Income.non_operating_expense,
                                                           Income.total_profit,
                                                           Income.net_profit,
                                                           Income.np_parent_company_owners
                                                           ])
                                                    , [trade_date]))
    valuation_sets = get_fundamentals(add_filter_trade(query(Valuation.__name__,
                                                             [Valuation.symbol,
                                                              Valuation.circulating_market_cap])
                                                       , [trade_date]))
    indicator_sets = get_fundamentals(add_filter_trade(query(Indicator.__name__,
                                                             [Indicator.symbol,
                                                              Indicator.adjusted_profit])
                                                       , [trade_date]))

    # 合并
    tp_earning = pd.merge(cash_flow_sets, balance_sets, on="symbol")
    tp_earning = pd.merge(tp_earning, income_sets, on="symbol")
    tp_earning = pd.merge(tp_earning, valuation_sets, on="symbol")
    tp_earning = pd.merge(tp_earning, indicator_sets, on="symbol")
    tp_earning = tp_earning[-tp_earning.duplicated()]
    # tp_earning.set_index('symbol', inplace=True)

    # TTM数据
    ttm_factors = {Balance.__name__: [Balance.symbol,
                                      Balance.total_assets,
                                      Balance.equities_parent_company_owners,
                                      Balance.total_owner_equities
                                      ],
                   CashFlow.__name__: [CashFlow.symbol,
                                       CashFlow.cash_and_equivalents_at_end],
                   Income.__name__: [Income.symbol,
                                     Income.total_operating_revenue,
                                     Income.operating_revenue,
                                     Income.interest_income,
                                     Income.total_operating_cost,
                                     Income.operating_cost,
                                     Income.financial_expense,
                                     Income.invest_income_associates,
                                     Income.operating_profit,
                                     Income.non_operating_revenue,
                                     Income.non_operating_expense,
                                     Income.total_profit,
                                     Income.net_profit,
                                     Income.np_parent_company_owners
                                     ]
                   }
    ttm_earning = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    ttm_earning = ttm_earning[-ttm_earning.duplicated()]

    ## 5年TTM数据
    ttm_factors = {Balance.__name__: [Balance.symbol,
                                      Balance.total_assets,
                                      Balance.total_owner_equities],
                   CashFlow.__name__: [CashFlow.symbol,
                                       CashFlow.cash_and_equivalents_at_end],
                   Income.__name__: [Income.symbol,
                                     Income.net_profit,]
                   }
    # 通过cache_data.set_cache， 会使得index的name丢失
    ttm_earning_5y = get_ttm_fundamental([], ttm_factors, trade_date, year=5).reset_index()
    ttm_earning_5y = ttm_earning_5y[-ttm_earning_5y.duplicated()]

    return tp_earning, ttm_earning_5y, ttm_earning


def prepare_calculate(trade_date):
    # earning
    tp_earning, ttm_earning_5y, ttm_earning = get_basic_earning(trade_date)
    if len(tp_earning) <= 0 or len(ttm_earning_5y) <= 0 or len(ttm_earning) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date) + "1", trade_date, tp_earning.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "2", trade_date, ttm_earning_5y.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + "3", trade_date, ttm_earning.to_json(orient='records'))
        factor_earning.factor_calculate.delay(date_index=trade_date, session=session)
        time6 = time.time()
        print('earning_cal_time:{}'.format(time6 - tic))


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
        processor = factor_earning.FactorEarning('factor_earning')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
