#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: 0.1
@author: li
@file: historical_value.py
@time: 2019-01-28 11:33
"""
import sys
from datetime import datetime

sys.path.append("..")

import math
import numpy as np
from vision.fm.signletion_engine import *
from factor.utillities.calc_tools import CalcTools

import json
from pandas.io.json import json_normalize

from factor import app
from factor.factor_base import FactorBase
from factor.ttm_fundamental import *
from vision.fm.signletion_engine import *
from factor.utillities import trade_date as td
from ultron.cluster.invoke.cache_data import cache_data


class HistoricalValue(FactorBase):

    def __init__(self, name):
        super(HistoricalValue, self).__init__(name)

    # 构建因子表
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
                    `PSIndu` decimal(19,4) NOT NULL,
                    `EarnToPrice` decimal(19,4),
                    `PEIndu` decimal(19,4),
                    `PEG3YChgTTM` decimal(19,4),
                    `PEG5YChgTTM` decimal(19, 4),
                    `PBIndu` decimal(19,4),
                    `historical_value_lcap_latest` decimal(19,4),
                    `historical_value_lflo_latest` decimal(19,4),
                    `historical_value_nlsize_latest` decimal(19,4),
                    `PCFIndu` decimal(19,4),
                    `CEToPTTM` decimal(19,4),
                    `historical_value_ctop_latest` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(HistoricalValue, self)._create_tables(create_sql, drop_sql)

    def ps_indu(self, tp_historical_value, factor_historical_value):
        """
        PEIndu， 市销率，以及同行业所有的公司的市销率
        # (PS – PS 的行业均值)/PS 的行业标准差
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        # 行业均值，行业标准差
        columns_lists = ['symbol', 'ps', 'isymbol']
        historical_value = tp_historical_value.loc[:, columns_lists]
        historical_value_grouped = historical_value.groupby('isymbol')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"ps": "ps_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"ps": "ps_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isymbol')
        historical_value = historical_value.merge(historical_value_mean, on='isymbol')

        historical_value['PSIndu'] = (historical_value['ps'] - historical_value['ps_mean']) / historical_value["ps_std"]
        historical_value = historical_value.drop(columns=['ps', 'isymbol', 'ps_mean', 'ps_std'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def etop(self, tp_historical_value, factor_historical_value):
        """
        收益市值比
        # 收益市值比= 净利润/总市值
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'net_profit', 'market_cap']
        historical_value = tp_historical_value.loc[:, columns_lists]

        historical_value['EarnToPrice'] = np.where(CalcTools.is_zero(historical_value['market_cap']),
                                                             0,
                                                             historical_value['net_profit'] /
                                                             historical_value['market_cap'])

        historical_value = historical_value.drop(columns=['net_profit', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    # 5年平均收益市值比 = 近5年净利润 / 近5年总市值
    def etp5(self, tp_historical_value, factor_historical_value):
        columns_lists = ['symbol', 'net_profit_5', 'circulating_market_cap_5', 'market_cap_5']
        historical_value = tp_historical_value.loc[:, columns_lists]

        fun = lambda x: x[0] / x[1] if x[1] is not None and x[1] != 0 else (x[0] / x[2] if x[2] is not None and x[2] !=0 else None)

        historical_value['historical_value_etp5_ttm'] = historical_value[['net_profit_5', 'circulating_market_cap_5', 'market_cap_5']].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['net_profit_5', 'circulating_market_cap_5', 'market_cap_5'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def pe_indu(self, tp_historical_value, factor_historical_value):
        """
        # (PE – PE 的行业均值)/PE 的行业标准差
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'pe', 'isymbol']
        historical_value = tp_historical_value.loc[:, columns_lists]
        historical_value_grouped = historical_value.groupby('isymbol')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"pe": "pe_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"pe": "pe_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isymbol')
        historical_value = historical_value.merge(historical_value_mean, on='isymbol')
        historical_value['PEIndu'] = (historical_value['pe'] - historical_value['pe_mean']) / historical_value["pe_std"]
        historical_value = historical_value.drop(columns=['pe', 'isymbol', 'pe_mean', 'pe_std'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def peg_3y(self, tp_historical_value, factor_historical_value):
        """
        # 市盈率/归属于母公司所有者净利润 3 年复合增长率
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'pe', 'np_parent_company_owners', 'np_parent_company_owners_3']
        historical_value = tp_historical_value.loc[:, columns_lists]

        tmp = np.where(CalcTools.is_zero(historical_value['np_parent_company_owners_3']), 0,
                       (historical_value['np_parent_company_owners'] / historical_value['np_parent_company_owners_3']))
        historical_value['PEG3YChgTTM'] = tmp / abs(tmp) * pow(abs(tmp), 1 / 3.0) - 1

        historical_value = historical_value.drop(
            columns=['pe', 'np_parent_company_owners', 'np_parent_company_owners_3'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def peg_5y(self, tp_historical_value, factor_historical_value):
        """
        # 市盈率/归属于母公司所有者净利润 5 年复合增长率
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'pe', 'np_parent_company_owners', 'np_parent_company_owners_5']
        historical_value = tp_historical_value.loc[:, columns_lists]

        tmp = np.where(CalcTools.is_zero(historical_value['np_parent_company_owners_5']), 0,
                       (historical_value['np_parent_company_owners'] / historical_value['np_parent_company_owners_5']))
        historical_value['PEG5YChgTTM'] = tmp / abs(tmp) * pow(abs(tmp), 1 / 5.0) - 1

        historical_value = historical_value.drop(
            columns=['pe', 'np_parent_company_owners', 'np_parent_company_owners_5'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def pb_indu(self, tp_historical_value, factor_historical_value):
        """
        # (PB – PB 的行业均值)/PB 的行业标准差
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'pb', 'isymbol']
        # 行业均值, 行业标准差
        historical_value = tp_historical_value.loc[:, columns_lists]
        historical_value_grouped = historical_value.groupby('isymbol')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"pb": "pb_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"pb": "pb_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isymbol')
        historical_value = historical_value.merge(historical_value_mean, on='isymbol')
        historical_value['PBIndu'] = (historical_value['pb'] - historical_value['pb_mean']) / historical_value["pb_std"]
        historical_value = historical_value.drop(columns=['pb', 'isymbol', 'pb_mean', 'pb_std'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def lcap(self, tp_historical_value, factor_historical_value):
        """
        总市值的对数
        # 对数市值 即市值的对数
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'market_cap']
        historical_value = tp_historical_value.loc[:, columns_lists]
        historical_value['historical_value_lcap_latest'] = historical_value['market_cap'].map(lambda x: math.log(abs(x)))
        historical_value = historical_value.drop(columns=['market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def lflo(self, tp_historical_value, factor_historical_value):
        """
        流通总市值的对数
        # 对数市值 即流通市值的对数
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'circulating_market_cap']
        historical_value = tp_historical_value.loc[:, columns_lists]

        historical_value['historical_value_lflo_latest'] = historical_value['circulating_market_cap'].map(lambda x: math.log(abs(x)))
        historical_value = historical_value.drop(columns=['circulating_market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def nlsize(self, tp_historical_value, factor_historical_value):
        """
        对数市值开立方
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'historical_value_lcap_latest']  # 对数市值
        historical_value = tp_historical_value.loc[:, columns_lists]
        historical_value['historical_value_nlsize_latest'] = historical_value['historical_value_lcap_latest'].map(lambda x: pow(math.log(abs(x)), 1/3.0))
        historical_value = historical_value.drop(columns=['historical_value_lcap_latest'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def pcf_indu(self, tp_historical_value, factor_historical_value):
        """
        # (PCF – PCF 的行业均值)/PCF 的行业标准差
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'pcf', 'isymbol']
        # 行业均值, 行业标准差
        historical_value = tp_historical_value.loc[:, columns_lists]
        historical_value_grouped = historical_value.groupby('isymbol')
        historical_value_mean = historical_value_grouped.mean()
        historical_value_std = historical_value_grouped.std()
        historical_value_std = historical_value_std.rename(columns={"pcf": "pcf_std"}).reset_index()
        historical_value_mean = historical_value_mean.rename(columns={"pcf": "pcf_mean"}).reset_index()
        historical_value = historical_value.merge(historical_value_std, on='isymbol')
        historical_value = historical_value.merge(historical_value_mean, on='isymbol')

        historical_value['PCFIndu'] = (historical_value['pcf'] - historical_value['pcf_mean']) / historical_value[
            "pcf_std"]
        historical_value = historical_value.drop(columns=['pcf', 'isymbol', 'pcf_mean', 'pcf_std'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    def cetop(self, tp_historical_value, factor_historical_value):
        """
        # 经营活动产生的现金流量净额与市值比
        :param tp_historical_value:
        :param factor_historical_value:
        :return:
        """
        columns_lists = ['symbol', 'net_operate_cash_flow', 'market_cap']
        historical_value = tp_historical_value.loc[:, columns_lists]

        historical_value['CEToPTTM'] = np.where(CalcTools.is_zero(historical_value['market_cap']), 0,
                                                                  historical_value['net_operate_cash_flow'] /
                                                                  historical_value['market_cap'])

        historical_value = historical_value.drop(columns=['net_operate_cash_flow', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")

        return factor_historical_value

    # 现金流市值比 = 每股派现 * 分红前总股本/总市值
    def ctop(self, tp_historical_value, factor_historical_value):
        columns_lists = ['symbol', 'pcd', 'sbd', 'circulating_market_cap', 'market_cap']
        historical_value = tp_historical_value.loc[:, columns_lists]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop_latest'] = historical_value[['pcd', 'sbd', 'circulating_market_cap', 'market_cap']].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap', 'market_cap'], axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value

    # 5 年平均现金流市值比  = 近5年每股派现 * 分红前总股本/近5年总市值
    def ctop5(self, tp_historical_value, factor_historical_value):
        columns_lists = ['symbol', 'pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5']
        historical_value = tp_historical_value.loc[:, columns_lists]

        fun = lambda x: x[0] * x[1] / x[2] if x[2] is not None and x[2] != 0 else (
            x[0] * x[1] / x[3] if x[3] is not None and x[3] != 0 else None)

        historical_value['historical_value_ctop5_latest'] = historical_value[
            ['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5']].apply(fun, axis=1)

        historical_value = historical_value.drop(columns=['pcd', 'sbd', 'circulating_market_cap_5', 'market_cap_5'],
                                                 axis=1)
        factor_historical_value = pd.merge(factor_historical_value, historical_value, on="symbol")
        return factor_historical_value


def calculate(trade_date, valuation_sets, historical_value):
    """

    :param trade_date:
    :return:
    """
    # valuation_sets, ttm_factor_sets, cash_flow_sets, income_sets = self.get_basic_data(trade_date)
    # valuation_sets = pd.merge(valuation_sets, income_sets, on='symbol')
    # valuation_sets = pd.merge(valuation_sets, ttm_factor_sets, on='symbol')
    # valuation_sets = pd.merge(valuation_sets, cash_flow_sets, on='symbol')
    if len(valuation_sets) <= 0:
        print("%s has no data" % trade_date)
        return

    # psindu
    factor_historical_value = historical_value.ps_indu(valuation_sets, valuation_sets)
    factor_historical_value = historical_value.etop(valuation_sets, factor_historical_value)
    # factor_historical_value = historical_value.etp5(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.pe_indu(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.peg_3y(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.peg_5y(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.pb_indu(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.lcap(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.lflo(factor_historical_value, factor_historical_value)
    factor_historical_value = historical_value.nlsize(factor_historical_value, factor_historical_value)
    factor_historical_value = historical_value.pcf_indu(valuation_sets, factor_historical_value)
    factor_historical_value = historical_value.cetop(factor_historical_value, factor_historical_value)
    factor_historical_value = historical_value.ctop(valuation_sets, factor_historical_value)
    # factor_historical_value = historical_value.ctop5(valuation_sets, factor_historical_value)

    # etp5 因子没有提出， 使用该部分的时候， 数据库字段需要添加
    # factor_historical_value = factor_historical_value[['symbol', 'PSIndu',
    #                                                    'historical_value_etp5_ttm',
    #                                                    'EarnToPrice',
    #                                                    'PEIndu', 'PEG3YChgTTM',
    #                                                    'PEG5YChgTTM', 'PBIndu',
    #                                                    'historical_value_lcap_latest','historical_value_lflo_latest',
    #                                                    'historical_value_nlsize_latest',
    #                                                    'PCFIndu',
    #                                                    'CEToPTTM',
    #                                                    'historical_value_ctop_latest',
    #                                                    'historical_value_ctop5_latest']]
    factor_historical_value = factor_historical_value[['symbol',
                                                       'PSIndu',
                                                       'EarnToPrice',
                                                       'PEIndu',
                                                       'PEG3YChgTTM',
                                                       'PEG5YChgTTM',
                                                       'PBIndu',
                                                       'historical_value_lcap_latest',
                                                       'historical_value_lflo_latest',
                                                       'historical_value_nlsize_latest',
                                                       'PCFIndu',
                                                       'CEToPTTM',
                                                       'historical_value_ctop_latest']]

    factor_historical_value['id'] = factor_historical_value['symbol'] + str(trade_date)
    factor_historical_value['trade_date'] = str(trade_date)
    historical_value._storage_data(factor_historical_value, trade_date)


def do_update(self, start_date, end_date, count):
    # 读取本地交易日
    trade_date_sets = self._trade_date.trade_date_sets_ago(start_date, end_date, count)
    for trade_date in trade_date_sets:
        print('因子计算日期： %s' % trade_date)
        self.calculate(trade_date)
    print('----->')


@app.task()
def factor_calculate(**kwargs):
    print("history_value_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    historical_value = HistoricalValue('factor_historical_value')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session, date_index)
    total_history_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_history_value_data {}".format(len(total_history_data)))
    calculate(date_index, total_history_data, historical_value)


