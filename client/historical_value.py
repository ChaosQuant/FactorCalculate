#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: historical_value.py
@time: 2019-07-16 19:48
"""
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import time
import collections
import argparse
from datetime import datetime, timedelta
from factor import historical_value
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


def get_basic_history_value_data(trade_date):
    """
    获取基础数据
    按天获取当天交易日所有股票的基础数据
    :param trade_date: 交易日
    :return:
    """
    # PS, PE, PB, PCF
    valuation_sets = get_fundamentals(add_filter_trade(query(Valuation._name_,
                                                             [Valuation.symbol,
                                                              Valuation.pe,
                                                              Valuation.ps,
                                                              Valuation.pb,
                                                              Valuation.pcf,
                                                              Valuation.market_cap,
                                                              Valuation.circulating_market_cap]), [trade_date]))

    cash_flow_sets = get_fundamentals(add_filter_trade(query(CashFlow._name_,
                                                             [CashFlow.symbol,
                                                              CashFlow.goods_sale_and_service_render_cash]), [trade_date]))

    income_sets = get_fundamentals(add_filter_trade(query(Income._name_,
                                                          [Income.symbol,
                                                           Income.net_profit]), [trade_date]))

    industry_set = ['801010', '801020', '801030', '801040', '801050', '801080', '801110', '801120', '801130',
                    '801140', '801150', '801160', '801170', '801180', '801200', '801210', '801230', '801710',
                    '801720', '801730', '801740', '801750', '801760', '801770', '801780', '801790', '801880',
                    '801890']
    sw_industry = get_fundamentals(add_filter_trade(query(Industry._name_,
                                                          [Industry.symbol,
                                                           Industry.isymbol]), [trade_date]))
    # TTM计算
    ttm_factors = {Income._name_: [Income.symbol,
                                   Income.np_parent_company_owners],
                   CashFlow._name_:[CashFlow.symbol,
                                    CashFlow.net_operate_cash_flow]
                   }

    ttm_factors_sum_list = {Income._name_:[Income.symbol,
                                           Income.net_profit,  # 净利润
                                        ],}

    trade_date_2y = get_trade_date(trade_date, 2)
    trade_date_3y = get_trade_date(trade_date, 3)
    trade_date_4y = get_trade_date(trade_date, 4)
    trade_date_5y = get_trade_date(trade_date, 5)
    # print(trade_date_2y, trade_date_3y, trade_date_4y, trade_date_5y)

    ttm_factor_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    ttm_factor_sets_3 = get_ttm_fundamental([], ttm_factors, trade_date_3y).reset_index()
    ttm_factor_sets_5 = get_ttm_fundamental([], ttm_factors, trade_date_5y).reset_index()
    # ttm 周期内计算需要优化
    # ttm_factor_sets_sum = get_ttm_fundamental([], ttm_factors_sum_list, trade_date, 5).reset_index()

    factor_sets_sum = get_fundamentals(add_filter_trade(query(Valuation._name_,
                                                              [Valuation.symbol,
                                                               Valuation.market_cap,
                                                               Valuation.circulating_market_cap,
                                                               Valuation.trade_date]),
                                                        [trade_date_2y, trade_date_3y, trade_date_4y, trade_date_5y]))

    factor_sets_sum_1 = factor_sets_sum.groupby('symbol')['market_cap'].sum().reset_index().rename(columns={"market_cap": "market_cap_sum",})
    factor_sets_sum_2 = factor_sets_sum.groupby('symbol')['circulating_market_cap'].sum().reset_index().rename(columns={"circulating_market_cap": "circulating_market_cap_sum",})

    # print(factor_sets_sum_1)
    # 根据申万一级代码筛选
    sw_industry = sw_industry[sw_industry['isymbol'].isin(industry_set)]

    # 合并价值数据和申万一级行业
    valuation_sets = pd.merge(valuation_sets, sw_industry, on='symbol')
    # valuation_sets = pd.merge(valuation_sets, sw_industry, on='symbol', how="outer")

    ttm_factor_sets = ttm_factor_sets.drop(columns={"trade_date"})
    ttm_factor_sets_3 = ttm_factor_sets_3.rename(columns={"np_parent_company_owners": "np_parent_company_owners_3"})
    ttm_factor_sets_3 = ttm_factor_sets_3.drop(columns={"trade_date"})

    ttm_factor_sets_5 = ttm_factor_sets_5.rename(columns={"np_parent_company_owners": "np_parent_company_owners_5"})
    ttm_factor_sets_5 = ttm_factor_sets_5.drop(columns={"trade_date"})

    # ttm_factor_sets_sum = ttm_factor_sets_sum.rename(columns={"net_profit": "net_profit_5"})
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_3, on='symbol')
    ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_5, on='symbol')
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_sum, on='symbol')
    ttm_factor_sets = pd.merge(ttm_factor_sets, factor_sets_sum_1, on='symbol')
    ttm_factor_sets = pd.merge(ttm_factor_sets, factor_sets_sum_2, on='symbol')
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_3, on='symbol', how='outer')
    # ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_5, on='symbol', how='outer')

    return valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets


def prepare_calculate(trade_date):
    # history_value
    valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets = get_basic_history_value_data(trade_date)
    valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
    valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return
    else:
        tic = time.time()
        session = str(int(time.time() * 1000000 + datetime.now().microsecond))
        cache_data.set_cache(session + str(trade_date), trade_date, valuation_sets.to_json(orient='records'))
        historical_value.factor_calculate.delay(date_index=trade_date, session=session)
        time2 = time.time()
        print('history_cal_time:{}'.format(time2 - tic))


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
        processor = historical_value.HistoricalValue('factor_historical_value')
        processor.create_dest_tables()
        do_update(args.start_date, end_date, args.count)
    if args.update:
        do_update(args.start_date, end_date, args.count)
