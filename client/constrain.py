#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: constrain.py
@time: 2019-07-16 19:22
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import factor_constrain
from factor.ttm_fundamental import *
from vision.file_unit.balance import Balance
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


def get_basic_constrain(trade_date):
    # 读取当前因子
    # 资产负债
    balance_sets = get_fundamentals(add_filter_trade(query(Balance._name_,
                                                           [Balance.symbol,
                                                            Balance.total_current_liability,
                                                            Balance.total_liability,
                                                            Balance.total_assets,
                                                            Balance.total_current_assets,
                                                            Balance.fixed_assets,
                                                            Balance.interest_bearing_liability
                                                            ]), [trade_date]))
    balance_sets = balance_sets[-balance_sets.duplicated()]

    # TTM计算
    ttm_factors = {Income._name_: [Income.symbol,
                                   Income.operating_cost,
                                   Income.operating_revenue,
                                   Income.operating_tax_surcharges,
                                   Income.total_operating_revenue,
                                   Income.total_operating_cost,
                                   Income.financial_expense,
                                   Income.sale_expense,
                                   Income.administration_expense
                                   ]}

    ttm_constrain_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    ttm_constrain_sets = ttm_constrain_sets[-ttm_constrain_sets.duplicated()]

    return balance_sets, ttm_constrain_sets


def prepare_calculate(trade_date):
    # factor_constrain
    balance_sets, ttm_factors_sets = get_basic_constrain(trade_date)
    if len(balance_sets) <= 0 or len(ttm_factors_sets) <=0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))

        cache_data.set_cache(session + str(trade_date) + '1', trade_date, balance_sets.to_json(orient='records'))
        cache_data.set_cache(session + str(trade_date) + '2', trade_date, ttm_factors_sets.to_json(orient='records'))
        factor_constrain.factor_calculate.delay(date_index=trade_date, session=session)
        time5 = time.time()
        print('constrain_cal_time:{}'.format(time5 - tic))


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
        processor = factor_constrain.FactorConstrain('factor_constrain')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
