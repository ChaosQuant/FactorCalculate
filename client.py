import pdb
import collections
import time

from datetime import datetime, timedelta
from factor import factor_growth, historical_value, factor_per_share_indicators, factor_cash_flow, factor_contrarian, factor_earning
from factor.ttm_fundamental import *
from vision.file_unit.balance import Balance
from vision.file_unit.cash_flow import CashFlow
from vision.file_unit.income import Income
from vision.file_unit.valuation import Valuation
from vision.file_unit.industry import Industry
from vision.file_unit.indicator import Indicator

from factor.utillities.trade_date import TradeDate
from ultron.cluster.invoke.cache_data import cache_data
from ultron.utilities.short_uuid import unique_machine, decode


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


def get_basic_cash_flow(trade_date):
    """
    获取cash flow所需要的因子
    :param trade_date:
    :return:
    """
    cash_flow_sets = get_fundamentals(add_filter_trade(query(CashFlow.__name__,
                                                             [CashFlow.symbol, CashFlow.net_operate_cash_flow,
                                                              CashFlow.goods_sale_and_service_render_cash])
                                                       , [trade_date]))
    income_sets = get_fundamentals(add_filter_trade(query(Income.__name__,
                                                          [Income.symbol, Income.operating_revenue,
                                                           Income.total_operating_cost,
                                                           Income.total_operating_revenue]), [trade_date]))
    valuation_sets = get_fundamentals(add_filter_trade(query(Valuation.__name__,
                                                             [Valuation.symbol, Valuation.market_cap,
                                                              Valuation.circulating_market_cap]), [trade_date]))

    # 合并
    tp_cash_flow = pd.merge(cash_flow_sets, income_sets, on="symbol")
    tp_cash_flow = tp_cash_flow[-tp_cash_flow.duplicated()]

    ttm_factors = {Balance.__name__: [Balance.symbol, Balance.total_liability,
                                      Balance.shortterm_loan,
                                      Balance.longterm_loan,
                                      Balance.total_current_liability,
                                      Balance.net_liability,
                                      Balance.total_current_assets,
                                      Balance.interest_bearing_liability,
                                      Balance.total_assets],
                   CashFlow.__name__: [CashFlow.symbol,
                                       CashFlow.net_operate_cash_flow,
                                       CashFlow.goods_sale_and_service_render_cash,
                                       CashFlow.cash_and_equivalents_at_end],
                   Income.__name__: [Income.symbol, Income.operating_revenue, Income.total_operating_revenue,
                                     Income.total_operating_cost, Income.net_profit,
                                     Income.np_parent_company_owners]
                   }
    ttm_factor_sets = get_ttm_fundamental([], ttm_factors, trade_date).reset_index()
    ttm_factor_sets = ttm_factor_sets[-ttm_factor_sets.duplicated()]

    # 合并
    ttm_factor_sets = pd.merge(ttm_factor_sets, valuation_sets, on="symbol")

    return tp_cash_flow, ttm_factor_sets


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
    ttm_factors = {Income._name_: [Income.symbol, Income.operating_cost,
                                   Income.operating_revenue,
                                   Income.operating_tax_surcharges,
                                   Income.total_operating_revenue,
                                   Income.total_operating_cost,
                                   Income.financial_expense,
                                   Income.sale_expense,
                                   Income.administration_expense
                                   ]}

    ttm_factors_sets = get_ttm_fundamental([], ttm_factors, trade_date)
    ttm_factors_sets = ttm_factors_sets[-ttm_factors_sets.duplicated()]

    return balance_sets, ttm_factors_sets


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
                                      Balance.total_owner_equities
                                      ],
                   CashFlow.__name__: [CashFlow.symbol,
                                       CashFlow.cash_and_equivalents_at_end],
                   Income.__name__: [Income.symbol,
                                     Income.net_profit,
                                     ]
                   }
    # 通过cache_data.set_cache， 会使得index的name丢失
    ttm_earning_5y = get_ttm_fundamental([], ttm_factors, trade_date, year=5).reset_index()
    ttm_earning_5y = ttm_earning_5y[-ttm_earning_5y.duplicated()]

    return tp_earning, ttm_earning_5y, ttm_earning


if __name__ == '__main__':
    session1 = str('156099868869460811')
    session2 = str('156099868869460812')
    # session3 = str('156099868869460813')
    session4 = str('156099868869460814')
    session5 = str('156099868869460815')
    session6 = str('156099868869460816')
    # session = str(int(time.time() * 1000000 + datetime.now().microsecond))
    start_date = 20100101
    end_date = 20190101
    count = 5
    rebuild = True
    _trade_date = TradeDate()
    trade_date_sets = _trade_date.trade_date_sets_ago(start_date, end_date, count)
    if rebuild is True:
        # create database table
        # growth
        growth = factor_growth.Growth('factor_growth')
        growth.create_dest_tables()

        # historical value
        history_value = historical_value.HistoricalValue('factor_historical_value')
        history_value.create_dest_tables()

        # scale
        # scale = factor_per_share_indicators.PerShareIndicators('factor_scale')
        # scale.create_dest_tables()

        # cash flow
        cash_flow = factor_cash_flow.FactorCashFlow('factor_cash_flow')
        cash_flow.create_dest_tables()

        # constrain
        constrain = factor_contrarian.FactorContrarian('factor_constrain')
        constrain.create_dest_tables()

        # earning
        earning = factor_earning.FactorEarning('factor_earning')
        earning.create_dest_tables()

    for date_index in trade_date_sets:
        # factor_growth
        start_time = time.time()
        ttm_factor_sets, balance_sets = get_basic_growth_data(date_index)
        growth_sets = pd.merge(ttm_factor_sets, balance_sets, on='symbol')
        cache_data.set_cache(session1, date_index, growth_sets.to_json(orient='records'))
        factor_growth.factor_calculate(date_index=date_index, session=session1)
        time1 = time.time()
        print('growth_cal_time:{}'.format(time1 - start_time))

        # history_value
        valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets = get_basic_history_value_data(date_index)
        valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
        valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
        valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
        cache_data.set_cache(session2, date_index, valuation_sets.to_json(orient='records'))
        historical_value.factor_calculate(date_index=date_index, session=session2)
        time2 = time.time()
        print('history_cal_time:{}'.format(time2 - time1))

        # # scale
        # valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets, balance_sets = get_basic_scale_data(date_index)
        # valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
        # valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
        # valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
        # valuation_sets = pd.merge(valuation_sets, balance_sets, on='symbol')
        # cache_data.set_cache(session3, date_index, valuation_sets.to_json(orient='records'))
        # factor_per_share_indicators.factor_calculate(date_index=date_index, session=session3)

        # cash flow
        tp_cash_flow, ttm_factor_sets = get_basic_cash_flow(date_index)
        cache_data.set_cache(session4 + "1", date_index, tp_cash_flow.to_json(orient='records'))
        cache_data.set_cache(session4 + "2", date_index, ttm_factor_sets.to_json(orient='records'))
        factor_cash_flow.factor_calculate(date_index=date_index, session=session4)
        time4 = time.time()
        print('cash_flow_cal_time:{}'.format(time4 - time2))

        # constrain
        balance_sets, ttm_factors_sets = get_basic_constrain(date_index)
        constrain_sets = pd.merge(balance_sets, ttm_factors_sets, on='symbol')
        cache_data.set_cache(session5, date_index, constrain_sets.to_json(orient='records'))
        factor_contrarian.factor_calculate(date_index=date_index, session=session5)
        time5 = time.time()
        print('constrain_cal_time:{}'.format(time5 - time4))

        # earning
        tp_earning, ttm_earning_5y, ttm_earning = get_basic_earning(date_index)
        cache_data.set_cache(session6 + "1", date_index, tp_earning.to_json(orient='records'))
        cache_data.set_cache(session6 + "2", date_index, ttm_earning_5y.to_json(orient='records'))
        cache_data.set_cache(session6 + "3", date_index, ttm_earning.to_json(orient='records'))
        factor_earning.factor_calculate(date_index=date_index, session=session6)
        time6 = time.time()
        print('earning_cal_time:{}'.format(time6 - time5))
        print('---------------------->')
