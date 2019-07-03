#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
每股指标
@version: ??
@author: li
@file: factor_per_share_indicators.py
@time: 2019-02-12 10:02
"""

import sys
import json
from pandas.io.json import json_normalize


sys.path.append("..")
from factor.ttm_fundamental import *
from factor.factor_base import FactorBase
from vision.fm.signletion_engine import *
from ultron.cluster.invoke.cache_data import cache_data


class PerShareIndicators(FactorBase):
    def __init__(self, name):
        super(PerShareIndicators, self).__init__(name)

    def create_dest_tables(self):
        """
        创建数据库表
        :return:
        """
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `eps_latest` decimal(19,4),
                    `diluted_eps_ttm` decimal(19,4),
                    `cash_equivalent_ps_latest` decimal(19,4),
                    `dividend_ps_latest` decimal(19,4),
                    `eps_ttm` decimal(19,4),
                    `net_asset_ps_latest` decimal(19,4),
                    `tor_ps_latest` decimal(19,4),
                    `tor_ps_ttm` decimal(19,4),
                    `operating_revenue_ps_ttm` decimal(19,4),
                    `operating_revenue_ps_latest` decimal(19,4),
                    `operating_profit_ps_ttm` decimal(19,4),
                    `operating_profit_ps_latest` decimal(19,4),
                    `capital_surplus_fund_ps_latest` decimal(19,4),
                    `surplus_reserve_fund_ps_latest` decimal(19,4),
                    `undivided_pro_fit_ps_latest` decimal(19,4),
                    `retained_earnings_ps_latest` decimal(19,4),
                    `oper_cash_flow_ps_ttm` decimal(19,4),
                    `cash_flow_ps_ttm` decimal(19,4),
                    `enterprise_fcfps_latest` decimal(19,4),
                    `shareholder_fcfps_latest` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(PerShareIndicators, self)._create_tables(create_sql, drop_sql)

    def eps(self, tp_share_indicators, factor_share_indicators):
        """
        基本每股收益
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'basic_eps']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        share_indicators['eps_latest'] = share_indicators['basic_eps']
        # share_indicators = share_indicators.drop(columns=['basic_eps'], axis=1)
        share_indicators = share_indicators[['symbol', 'eps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def diluted_eps(self, tp_share_indicators, factor_share_indicators):
        """
        稀释每股收益
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'diluted_eps']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        share_indicators['diluted_eps_ttm'] = share_indicators['diluted_eps']
        # share_indicators = share_indicators.drop(columns=['diluted_eps'], axis=1)
        share_indicators = share_indicators[['symbol', 'diluted_eps_ttm']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def cash_equivalent_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股现金及现金等价物余额
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'capitalization', 'cash_and_equivalents_at_end']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[1] / x[0] if x[0] and x[0] != 0 else None)
        share_indicators['cash_equivalent_ps_latest'] = share_indicators[
            ['capitalization', 'cash_and_equivalents_at_end']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['cash_and_equivalents_at_end'], axis=1)
        share_indicators = share_indicators[['symbol', 'cash_equivalent_ps_latest']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def dividend_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股股利（税前）
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'dividend_receivable']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        share_indicators['dividend_ps_latest'] = share_indicators['dividend_receivable']
        # share_indicators = share_indicators.drop(columns=['dividend_receivable'], axis=1)
        share_indicators = share_indicators[['symbol', 'dividend_ps_latest']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def eps_ttm(self, tp_share_indicators, factor_share_indicators):
        """
        每股收益 TTM
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'np_parent_company_owners_ttm', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else 0)
        share_indicators['eps_ttm'] = share_indicators[['np_parent_company_owners_ttm', 'capitalization']].apply(fun,
                                                                                                                 axis=1)

        # share_indicators = share_indicators.drop(columns=['np_parent_company_owners_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'eps_ttm']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def net_asset_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股净资产
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'total_owner_equities', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['net_asset_ps_latest'] = share_indicators[['total_owner_equities', 'capitalization']].apply(fun,
                                                                                                                   axis=1)

        # share_indicators = share_indicators.drop(columns=['total_owner_equities'], axis=1)
        share_indicators = share_indicators[['symbol', 'net_asset_ps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def tor_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股营业总收入
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'total_operating_revenue_ttm', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['tor_ps_ttm'] = share_indicators[
            ['total_operating_revenue_ttm', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['total_operating_revenue_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'tor_ps_ttm']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def tor_ps_latest(self, tp_share_indicators, factor_share_indicators):
        """
        每股营业总收入
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'total_operating_revenue', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['tor_ps_latest'] = share_indicators[['total_operating_revenue', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['total_operating_revenue'], axis=1)
        share_indicators = share_indicators[['symbol', 'tor_ps_latest']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def operating_revenue_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股营业收入
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue_ttm', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['operating_revenue_ps_ttm'] = share_indicators[
            ['operating_revenue_ttm', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_revenue_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'operating_revenue_ps_ttm']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def operating_revenue_ps_latest(self, tp_share_indicators, factor_share_indicators):
        """
        每股营业收入(最新)
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['operating_revenue_ps_latest'] = share_indicators[
            ['operating_revenue', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_revenue'], axis=1)
        share_indicators = share_indicators[['symbol', 'operating_revenue_ps_latest']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def operating_profit_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股营业利润
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'operating_profit_ttm', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['operating_profit_ps_ttm'] = share_indicators[
            ['operating_profit_ttm', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_profit_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'operating_profit_ps_ttm']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def operating_profit_ps_latest(self, tp_share_indicators, factor_share_indicators):
        """
        每股营业利润（最新）
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'operating_profit', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['operating_profit_ps_latest'] = share_indicators[
            ['operating_profit', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['operating_profit'], axis=1)
        share_indicators = share_indicators[['symbol', 'operating_profit_ps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def capital_surplus_fund_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股资本公积金
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'capital_reserve_fund', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['capital_surplus_fund_ps_latest'] = share_indicators[
            ['capital_reserve_fund', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['capital_reserve_fund'], axis=1)
        share_indicators = share_indicators[['symbol', 'capital_surplus_fund_ps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def surplus_reserve_fund_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股盈余公积金
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'surplus_reserve_fund', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['surplus_reserve_fund_ps_latest'] = share_indicators[['surplus_reserve_fund', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['surplus_reserve_fund'], axis=1)
        share_indicators = share_indicators[['symbol', 'surplus_reserve_fund_ps_latest']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def undivided_pro_fit_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股未分配利润
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'retained_profit', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['undivided_pro_fit_ps_latest'] = share_indicators[
            ['retained_profit', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['retained_profit'], axis=1)
        share_indicators = share_indicators[['symbol', 'undivided_pro_fit_ps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def retained_earnings_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股留存收益
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'surplus_reserve_fund_ps_latest', 'undivided_pro_fit_ps_latest']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        share_indicators['retained_earnings_ps_latest'] = share_indicators['undivided_pro_fit_ps_latest'] + \
                                                          share_indicators['surplus_reserve_fund_ps_latest']

        # share_indicators = share_indicators.drop(columns=['undivided_pro_fit_ps_latest', 'surplus_reserve_fund_ps_latest'], axis=1)
        share_indicators = share_indicators[['symbol', 'retained_earnings_ps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def oper_cash_flow_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股经营活动产生的现金流量净额
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'net_operate_cash_flow_ttm', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['oper_cash_flow_ps_ttm'] = share_indicators[
            ['net_operate_cash_flow_ttm', 'capitalization']].apply(
            fun,
            axis=1)

        # share_indicators = share_indicators.drop(columns=['net_operate_cash_flow_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'oper_cash_flow_ps_ttm']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def cash_flow_ps(self, tp_share_indicators, factor_share_indicators):
        """
        每股现金流量净额
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'n_change_in_cash_ttm', 'capitalization']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        fun = lambda x: (x[0] / x[1] if x[1] and x[1] != 0 else None)
        share_indicators['cash_flow_ps_ttm'] = share_indicators[['n_change_in_cash_ttm', 'capitalization']].apply(fun, axis=1)

        # share_indicators = share_indicators.drop(columns=['n_change_in_cash_ttm'], axis=1)
        share_indicators = share_indicators[['symbol', 'cash_flow_ps_ttm']]

        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def enterprise_fcfps(self, tp_share_indicators, factor_share_indicators):
        """
        每股企业自由现金流量
        缺每股企业自由现金流量
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'enterprise_fcfps']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        share_indicators['enterprise_fcfps_latest'] = share_indicators['enterprise_fcfps']
        # share_indicators = share_indicators.drop(columns=['enterprise_fcfps'], axis=1)
        share_indicators = share_indicators[['symbol', 'enterprise_fcfps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators

    def shareholder_fcfps(self, tp_share_indicators, factor_share_indicators):
        """
        每股股东自由现金流量
        缺每股股东自由现金流量
        :param tp_share_indicators:
        :param factor_share_indicators:
        :return:
        """
        columns_lists = ['symbol', 'shareholder_fcfps']
        share_indicators = tp_share_indicators.loc[:, columns_lists]
        share_indicators['shareholder_fcfps_latest'] = share_indicators['shareholder_fcfps']
        # share_indicators = share_indicators.drop(columns=['shareholder_fcfps'], axis=1)
        share_indicators = share_indicators[['symbol', 'shareholder_fcfps_latest']]
        factor_share_indicators = pd.merge(factor_share_indicators, share_indicators, on='symbol')
        return factor_share_indicators


def calculate(trade_date, valuation_sets, scale):
    """
    规模
    :param scale: 规模类
    :param valuation_sets: 基础数据
    :param trade_date: 交易日
    :return:
    """
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return

    # psindu
    factor_share_indicators = scale.eps(valuation_sets, valuation_sets)
    factor_share_indicators = scale.diluted_eps(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.cash_equivalent_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.dividend_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.eps_ttm(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.net_asset_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.tor_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.tor_ps_latest(factor_share_indicators, factor_share_indicators)   # memorydrror
    factor_share_indicators = scale.operating_revenue_ps(factor_share_indicators, factor_share_indicators)   # memoryerror
    factor_share_indicators = scale.operating_revenue_ps_latest(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.operating_profit_ps(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.operating_profit_ps_latest(valuation_sets, factor_share_indicators)
    factor_share_indicators = scale.capital_surplus_fund_ps(factor_share_indicators, factor_share_indicators) # memoryerror
    factor_share_indicators = scale.surplus_reserve_fund_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = scale.undivided_pro_fit_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = scale.retained_earnings_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = scale.oper_cash_flow_ps(factor_share_indicators, factor_share_indicators)  # memorydrror
    factor_share_indicators = scale.cash_flow_ps(factor_share_indicators, factor_share_indicators)  # memorydrror

    # factor_historical_value = factor_historical_value.drop(columns=['pb', 'pe', 'ps', 'pcf', 'market_cap',
    #                                                                 'circulating_market_cap', 'isymbol',
    #                                                                 'np_parent_company_owners',
    #                                                                 'np_parent_company_owners_3',
    #                                                                 'np_parent_company_owners_5',
    #                                                                 'net_operate_cash_flow', 'net_profit',
    #                                                                 'goods_sale_and_service_render_cash'])

    # factor_share_indicators = factor_share_indicators[['symbol',
    #                                                    'eps_latest',
    #                                                    'diluted_eps_ttm',
    #                                                    'cash_equivalent_ps_latest',
    #                                                    'dividend_ps_latest',
    #                                                    'eps_ttm',
    #                                                    'net_asset_ps_latest',
    #                                                    'tor_ps_latest',
    #                                                    'tor_ps_ttm',
    #                                                    'operating_revenue_ps_ttm',
    #                                                    'operating_revenue_ps_latest',
    #                                                    'operating_profit_ps_ttm',
    #                                                    'operating_profit_ps_latest',
    #                                                    'capital_surplus_fund_ps_latest',
    #                                                    'surplus_reserve_fund_ps_latest',
    #                                                    'undivided_pro_fit_ps_latest',
    #                                                    'retained_earnings_ps_latest',
    #                                                    'oper_cash_flow_ps_ttm',
    #                                                    'cash_flow_ps_ttm',
    #                                                    'enterprise_fcfps_latest',
    #                                                    'shareholder_fcfps_latest']]

    factor_share_indicators = factor_share_indicators[['symbol',
                                                       'eps_latest',
                                                       'diluted_eps_ttm',
                                                       'cash_equivalent_ps_latest',
                                                       'dividend_ps_latest',
                                                       'eps_ttm',
                                                       'net_asset_ps_latest',
                                                       'tor_ps_latest',
                                                       'tor_ps_ttm',
                                                       'operating_revenue_ps_ttm',
                                                       'operating_revenue_ps_latest',
                                                       'operating_profit_ps_ttm',
                                                       'operating_profit_ps_latest',
                                                       'capital_surplus_fund_ps_latest',
                                                       'surplus_reserve_fund_ps_latest',
                                                       'undivided_pro_fit_ps_latest',
                                                       'retained_earnings_ps_latest',
                                                       'oper_cash_flow_ps_ttm',
                                                       'cash_flow_ps_ttm']]

    factor_share_indicators['id'] = factor_share_indicators['symbol'] + str(trade_date)
    factor_share_indicators['trade_date'] = str(trade_date)
    scale._storage_data(factor_share_indicators, trade_date)


def do_update(self, start_date, end_date, count):
    # 读取本地交易日
    trade_date_sets = self._trade_date.trade_date_sets_ago(start_date, end_date, count)
    for trade_date in trade_date_sets:
        print('当前交易日： %s' % trade_date)
        self.calculate(trade_date)
    print('----->')


# @app.task()
def factor_calculate(**kwargs):
    print("scale_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    scale = PerShareIndicators('factor_scale')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session, date_index)
    total_growth_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_growth_data {}".format(len(total_growth_data)))
    calculate(date_index, total_growth_data, scale)
