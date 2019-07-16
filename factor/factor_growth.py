#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: factor_growth.py
@time: 2019-02-12 10:03
"""
import json
from sklearn import linear_model
from pandas.io.json import json_normalize

from factor import app
from factor.factor_base import FactorBase
from factor.ttm_fundamental import *
from ultron.cluster.invoke.cache_data import cache_data


class Growth(FactorBase):
    """
    历史成长
    """
    def __init__(self, name):
        super(Growth, self).__init__(name)

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
                    `NetAsset1YChg` decimal(19,4),
                    `TotalAsset1YChg` decimal(19,4),
                    `ORev1YChgTTM` decimal(19,4),
                    `OPft1YChgTTM` decimal(19,4),
                    `GrPft1YChgTTM` decimal(19,4),
                    `NetPft1YChgTTM` decimal(19,4),
                    `NetPftAP1YChgTTM` decimal(19,4),
                    `NetPft3YChgTTM` decimal(19,4),
                    `NetPft5YChgTTM` decimal(19,4),
                    `ORev3YChgTTM` decimal(19,4),
                    `ORev5YChgTTM` decimal(19,4),
                    `NetCF1YChgTTM` decimal(19,4),
                    `NetPftAPNNRec1YChgTTM` decimal(19,4),
                    `NetPft5YAvgChgTTM` decimal(19,4),
                    `StdUxpErn1YTTM` decimal(19,4),
                    `StdUxpGrPft1YTTM` decimal(19,4),
                    `FCF1YChgTTM` decimal(19,4),
                    `ICF1YChgTTM` decimal(19,4),
                    `OCF1YChgTTM` decimal(19,4),
                    `Sales5YChgTTM` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(Growth, self)._create_tables(create_sql, drop_sql)

    @staticmethod
    def historical_net_asset_grow_rate(tp_historical_growth):
        """
        净资产增长率
        :param tp_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'total_owner_equities', 'total_owner_equities_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]

        if len(historical_growth) <= 0:
            return

        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['NetAsset1YChg'] = historical_growth[['total_owner_equities',
                                                                'total_owner_equities_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['total_owner_equities',
                                                            'total_owner_equities_pre_year'], axis=1)
        # factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return historical_growth

    @staticmethod
    def historical_total_asset_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        总资产增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'total_assets', 'total_assets_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['TotalAsset1YChg'] = historical_growth[
            ['total_assets', 'total_assets_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['total_assets', 'total_assets_pre_year'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_revenue_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        营业收入增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue', 'operating_revenue_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['ORev1YChgTTM'] = historical_growth[
            ['operating_revenue', 'operating_revenue_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['operating_revenue', 'operating_revenue_pre_year'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_profit_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        营业利润增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'operating_profit', 'operating_profit_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1.0 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['OPft1YChgTTM'] = historical_growth[
            ['operating_profit', 'operating_profit_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['operating_profit', 'operating_profit_pre_year'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_total_profit_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        利润总额增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'total_profit', 'total_profit_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['GrPft1YChgTTM'] = historical_growth[
            ['total_profit', 'total_profit_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['total_profit', 'total_profit_pre_year'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_profit_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        净利润增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_profit', 'net_profit_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft1YChgTTM'] = historical_growth[
            ['net_profit', 'net_profit_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['net_profit', 'net_profit_pre_year'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_np_parent_company_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        归属母公司股东的净利润增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'np_parent_company_owners', 'np_parent_company_owners_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPftAP1YChgTTM'] = historical_growth[
            ['np_parent_company_owners', 'np_parent_company_owners_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(
            columns=['np_parent_company_owners', 'np_parent_company_owners_pre_year'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_profit_grow_rate_3y(tp_historical_growth, factor_historical_growth):
        """
        净利润3年复合增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_profit', 'net_profit_pre_year_3']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 3.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft3YChgTTM'] = historical_growth[
            ['net_profit', 'net_profit_pre_year_3']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['net_profit', 'net_profit_pre_year_3'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_profit_grow_rate_5y(tp_historical_growth, factor_historical_growth):
        """
        净利润5年复合增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_profit', 'net_profit_pre_year_5']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 5.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPft5YChgTTM'] = historical_growth[
            ['net_profit', 'net_profit_pre_year_5']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['net_profit', 'net_profit_pre_year_5'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_revenue_grow_rate_3y(tp_historical_growth, factor_historical_growth):
        """
        营业收入3年复合增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue', 'operating_revenue_pre_year_3']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 3.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['ORev3YChgTTM'] = historical_growth[
            ['operating_revenue', 'operating_revenue_pre_year_3']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['operating_revenue', 'operating_revenue_pre_year_3'],
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_operating_revenue_grow_rate_5y(tp_historical_growth, factor_historical_growth):
        """
        营业收入5年复合增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue', 'operating_revenue_pre_year_5']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: (pow((x[0] / x[1]), 1 / 5.0) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)

        historical_growth['ORev5YChgTTM'] = historical_growth[
            ['operating_revenue', 'operating_revenue_pre_year_5']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['operating_revenue', 'operating_revenue_pre_year_5'],
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_net_cash_flow_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        缺数据
        净现金流量增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'n_change_in_cash', 'n_change_in_cash_pre']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetCF1YChgTTM'] = historical_growth[
            ['n_change_in_cash', 'n_change_in_cash_pre']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['n_change_in_cash', 'n_change_in_cash_pre'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_np_parent_company_cut_yoy(tp_historical_growth, factor_historical_growth):
        """
        缺失数据
        归属母公司股东的净利润（扣除）同比增长
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'ni_attr_p_cut', 'ni_attr_p_cut_pre']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[0] is not None and x[1] is not None else None)
        historical_growth['NetPftAPNNRec1YChgTTM'] = historical_growth[
            ['ni_attr_p_cut', 'ni_attr_p_cut_pre']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['ni_attr_p_cut', 'ni_attr_p_cut_pre'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_egro(tp_historical_growth, factor_historical_growth):
        columns_lists = ['symbol', 'net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                         'net_profit_pre_year_3', 'net_profit_pre_year_4']
        regr = linear_model.LinearRegression()
        # 读取五年的时间和净利润
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return

        def has_non(a):
            tmp = 0
            for i in a.tolist():
                for j in i:
                    if j is None or j == 'nan':
                        tmp += 1
            if tmp >= 1:
                return True
            else:
                return False

        def fun2(x):
            aa = x[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3', 'net_profit_pre_year_4']].fillna('nan').values.reshape(-1, 1)
            if has_non(aa):
                return None
            else:
                regr.fit(aa, range(0, 5))
                return regr.coef_[-1]

        # fun = lambda x: (regr.coef_[-1] if regr.fit(x[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
        #                                                'net_profit_pre_year_3', 'net_profit_pre_year_4']].values.reshape(-1, 1),
        #                                             range(0, 5)) else None)

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3', 'net_profit_pre_year_4']].fillna('nan').mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] != 0 and x[1] is not None and x[0] is not None else None
        historical_growth['NetPft5YAvgChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        # historical_growth = historical_growth.drop(
        #     columns=['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2', 'net_profit_pre_year_3',
        #              'net_profit_pre_year_4', 'coefficient', 'mean'], axis=1)

        historical_growth = historical_growth[['symbol', 'NetPft5YAvgChgTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')

        return factor_historical_growth

    @staticmethod
    def historical_sue(tp_historical_growth, factor_historical_growth):
        """
        未预期盈余
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                         'net_profit_pre_year_3', 'net_profit_pre_year_4']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        historical_growth['mean'] = historical_growth[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                                                       'net_profit_pre_year_3', 'net_profit_pre_year_4']].fillna(0.0).mean(axis=1)
        historical_growth['std'] = historical_growth[['net_profit', 'net_profit_pre_year_1', 'net_profit_pre_year_2',
                                                      'net_profit_pre_year_3', 'net_profit_pre_year_4']].fillna(0.0).std(axis=1)

        fun = lambda x: (x[0] - x[1]) / x[2] if x[2] !=0 and x[1] is not None and x[0] is not None and x[2] is not None else None

        historical_growth['StdUxpErn1YTTM'] = historical_growth[['net_profit', 'mean', 'std']].apply(fun, axis=1)

        # historical_growth = historical_growth.drop(columns=['net_profit', 'std', 'mean'], axis=1)
        historical_growth = historical_growth[['symbol', 'StdUxpErn1YTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')

        return factor_historical_growth

    @staticmethod
    def historical_suoi(tp_historical_growth, factor_historical_growth):
        """
        未预期毛利
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue', 'operating_revenue_pre_year_1', 'operating_revenue_pre_year_2',
                         'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4', 'operating_revenue_pre_year_5',
                         'operating_cost', 'operating_cost_pre_year_1', 'operating_cost_pre_year_2',
                         'operating_cost_pre_year_3', 'operating_cost_pre_year_4', 'operating_cost_pre_year_5']

        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        historical_growth['gi_1'] = historical_growth['operating_revenue'] - historical_growth['operating_cost']
        historical_growth['gi_2'] = historical_growth['operating_revenue_pre_year_2'] - historical_growth[
            'operating_cost_pre_year_2']
        historical_growth['gi_3'] = historical_growth['operating_revenue_pre_year_3'] - historical_growth[
            'operating_cost_pre_year_3']
        historical_growth['gi_4'] = historical_growth['operating_revenue_pre_year_4'] - historical_growth[
            'operating_cost_pre_year_4']
        historical_growth['gi_5'] = historical_growth['operating_revenue_pre_year_5'] - historical_growth[
            'operating_cost_pre_year_5']

        historical_growth['mean'] = historical_growth[['gi_2', 'gi_3', 'gi_4', 'gi_5']].mean(axis=1)
        historical_growth['std'] = historical_growth[['gi_2', 'gi_3', 'gi_4', 'gi_5']].std(axis=1)

        fun = lambda x: ((x[0] - x[1]) / x[2] if x[2] != 0 and x[1] is not None and x[0] is not None and x[2] is not None else None)
        historical_growth['StdUxpGrPft1YTTM'] = historical_growth[['gi_1', 'mean', 'std']].apply(fun, axis=1)

        historical_growth = historical_growth[['symbol', 'StdUxpGrPft1YTTM']]
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')

        return factor_historical_growth

    @staticmethod
    def historical_financing_cash_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        筹资活动产生的现金流量净额增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_finance_cash_flow', 'net_finance_cash_flow_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['FCF1YChgTTM'] = historical_growth[
            ['net_finance_cash_flow', 'net_finance_cash_flow_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['net_finance_cash_flow', 'net_finance_cash_flow_pre_year'],
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_oper_cash_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        经营活动产生的现金流量净额
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_operate_cash_flow', 'net_operate_cash_flow_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['OCF1YChgTTM'] = historical_growth[
            ['net_operate_cash_flow', 'net_operate_cash_flow_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['net_operate_cash_flow', 'net_operate_cash_flow_pre_year'],
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_invest_cash_grow_rate(tp_historical_growth, factor_historical_growth):
        """
        投资活动产生的现金流量净额
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'net_invest_cash_flow', 'net_invest_cash_flow_pre_year']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['ICF1YChgTTM'] = historical_growth[
            ['net_invest_cash_flow', 'net_invest_cash_flow_pre_year']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['net_invest_cash_flow', 'net_invest_cash_flow_pre_year'],
                                                   axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def historical_sgro(tp_historical_growth, factor_historical_growth):
        """
        五年营业收入增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'operating_revenue', 'operating_revenue_pre_year_1', 'operating_revenue_pre_year_2',
                         'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4']
        regr = linear_model.LinearRegression()
        # 读取五年的时间和净利润
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return

        def has_non(a):
            tmp = 0
            for i in a.tolist():
                for j in i:
                    if j is None or j == 'nan':
                        tmp += 1
            if tmp >= 1:
                return True
            else:
                return False

        def fun2(x):
            aa = x[['operating_revenue', 'operating_revenue_pre_year_1', 'operating_revenue_pre_year_2',
                    'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4']].fillna('nan').values.reshape(-1, 1)
            if has_non(aa):
                return None
            else:
                regr.fit(aa, range(0, 5))
                return regr.coef_[-1]

        historical_growth['coefficient'] = historical_growth.apply(fun2, axis=1)
        historical_growth['mean'] = historical_growth[['operating_revenue', 'operating_revenue_pre_year_1',
                                                       'operating_revenue_pre_year_2', 'operating_revenue_pre_year_3',
                                                       'operating_revenue_pre_year_4']].fillna(0.0).mean(axis=1)

        fun1 = lambda x: x[0] / abs(x[1]) if x[1] is not None and x[0] is not None and x[1] != 0 else None
        historical_growth['Sales5YChgTTM'] = historical_growth[['coefficient', 'mean']].apply(fun1, axis=1)

        historical_growth = historical_growth.drop(
            columns=['operating_revenue', 'operating_revenue_pre_year_1', 'operating_revenue_pre_year_2',
                     'operating_revenue_pre_year_3', 'operating_revenue_pre_year_4', 'coefficient', 'mean'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')

        return factor_historical_growth

    # 分析师预期增长
    @staticmethod
    def fsalesg(tp_historical_growth, factor_historical_growth):
        """
        未来预期营收增长
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'sales_predict', 'sales_real']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['historical_financing_fearng_latest'] = historical_growth[
            ['sales_predict', 'sales_real']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['sales_predict', 'sales_real'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def fearng(tp_historical_growth, factor_historical_growth):
        """
        未来预期盈利增长
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'earnings_predict', 'earnings_real']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] - x[1]) / abs(x[1]) if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['historical_financing_fearng_latest'] = historical_growth[
            ['earnings_predict', 'earnings_real']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['earnings_predict', 'earnings_real'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth

    @staticmethod
    def egibs_long(tp_historical_growth, factor_historical_growth):
        """
        净利润增长率
        :param tp_historical_growth:
        :param factor_historical_growth:
        :return:
        """
        columns_lists = ['symbol', 'earnings', 'earnings_pre_year_3']
        historical_growth = tp_historical_growth.loc[:, columns_lists]
        if len(historical_growth) <= 0:
            return
        fun = lambda x: ((x[0] / x[1]) - 1 if x[1] and x[1] != 0 and x[1] is not None and x[0] is not None else None)
        historical_growth['NetPft1YChgTTM'] = historical_growth[
            ['earnings', 'earnings_pre_year_3']].apply(fun, axis=1)

        historical_growth = historical_growth.drop(columns=['earnings', 'earnings_pre_year_3'], axis=1)
        factor_historical_growth = pd.merge(factor_historical_growth, historical_growth, on='symbol')
        return factor_historical_growth


def calculate(trade_date, growth_sets, growth):
    """
    :param growth: 成长类
    :param growth_sets: 基础数据
    :param trade_date: 交易日
    :return:
        """
    if len(growth_sets) <= 0:
        print("%s has no data" % trade_date)
        return

    factor_historical_growth = growth.historical_net_asset_grow_rate(growth_sets)
    factor_historical_growth = growth.historical_total_asset_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_revenue_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_profit_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_total_profit_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_net_profit_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_np_parent_company_grow_rate(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_net_profit_grow_rate_3y(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_net_profit_grow_rate_5y(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_revenue_grow_rate_3y(growth_sets, factor_historical_growth)
    factor_historical_growth = growth.historical_operating_revenue_grow_rate_5y(factor_historical_growth,
                                                                                factor_historical_growth)
    factor_historical_growth = growth.historical_net_cash_flow_grow_rate(factor_historical_growth,
                                                                         factor_historical_growth)
    factor_historical_growth = growth.historical_np_parent_company_cut_yoy(factor_historical_growth,
                                                                           factor_historical_growth)
    factor_historical_growth = growth.historical_egro(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = growth.historical_sue(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = growth.historical_suoi(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = growth.historical_financing_cash_grow_rate(factor_historical_growth,
                                                                          factor_historical_growth)
    factor_historical_growth = growth.historical_oper_cash_grow_rate(factor_historical_growth,
                                                                     factor_historical_growth)
    factor_historical_growth = growth.historical_invest_cash_grow_rate(factor_historical_growth,
                                                                       factor_historical_growth)
    factor_historical_growth = growth.historical_sgro(factor_historical_growth, factor_historical_growth)
    factor_historical_growth = factor_historical_growth[['symbol',
                                                         'NetAsset1YChg',
                                                         'TotalAsset1YChg',
                                                         'ORev1YChgTTM',
                                                         'OPft1YChgTTM',
                                                         'GrPft1YChgTTM',
                                                         'NetPft1YChgTTM',
                                                         'NetPftAP1YChgTTM',
                                                         'NetPft3YChgTTM',
                                                         'NetPft5YChgTTM',
                                                         'ORev3YChgTTM',
                                                         'ORev5YChgTTM',
                                                         'NetCF1YChgTTM',
                                                         'NetPftAPNNRec1YChgTTM',
                                                         'NetPft5YAvgChgTTM',
                                                         'StdUxpErn1YTTM',
                                                         'StdUxpGrPft1YTTM',
                                                         'FCF1YChgTTM',
                                                         'ICF1YChgTTM',
                                                         'OCF1YChgTTM',
                                                         'Sales5YChgTTM']]

    factor_historical_growth['id'] = factor_historical_growth['symbol'] + str(trade_date)
    factor_historical_growth['trade_date'] = str(trade_date)
    growth._storage_data(factor_historical_growth, trade_date)


@app.task()
def factor_calculate(**kwargs):
    print("growth_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    growth = Growth('factor_growth')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session + str(date_index), date_index)
    total_growth_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_growth_data {}".format(len(total_growth_data)))
    calculate(date_index, total_growth_data, growth)

