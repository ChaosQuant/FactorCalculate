#!/usr/bin/env python
# coding=utf-8

import pdb
import sys
import numpy as np
import pandas as pd
import argparse
from datetime import datetime

sys.path.append("..")
import json
from pandas.io.json import json_normalize
from factor import app

from factor.factor_base import FactorBase
from factor.ttm_fundamental import *
from vision.fm.signletion_engine import *
from vision.utillities.calc_tools import CalcTools
from ultron.cluster.invoke.cache_data import cache_data


class FactorContrarian(FactorBase):
    def __init__(self, name):
        super(FactorContrarian, self).__init__(name)

    # 构建因子表
    def create_dest_tables(self):
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `SalesCostTTM` decimal(19,4),
                    `TaxRTTM` decimal(19,4),
                    `FinExpTTM` decimal(19,4),
                    `OperExpTTM` decimal(19,4),
                    `AdminExpTTM` decimal(19,4),
                    `PeridCostTTM` decimal(19,4),
                    `DTE` decimal(19,4),
                    `DA` decimal(19,4),
                    `IntBDToCap` decimal(19,4),
                     PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorContrarian, self)._create_tables(create_sql, drop_sql)

    # 销售成本率=营业成本(TTM)/营业收入(TTM)
    def sales_cost_ratio_ttm(self, tp_contrarian, factor_contrarian):
        columns_list = ['operating_cost', 'operating_revenue']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['SalesCostTTM'] = np.where(
            CalcTools.is_zero(contrarian['operating_revenue']),
            0, contrarian['operating_cost'] / contrarian['operating_revenue']
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 销售税金率=营业税金及附加(TTM)/营业收入(TTM)
    def tax_ratio_ttm(self, tp_contrarian, factor_contrarian):
        columns_list = ['operating_tax_surcharges', 'operating_revenue']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['TaxRTTM'] = np.where(
            CalcTools.is_zero(contrarian['operating_revenue']), 0,
            contrarian['operating_tax_surcharges'] / contrarian['operating_revenue']
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 财务费用与营业总收入之比=财务费用(TTM)/营业总收入(TTM)
    def financial_expense_rate_ttm(self, tp_contrarian, factor_contrarian):
        columns_list = ['financial_expense', 'total_operating_cost']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['FinExpTTM'] = np.where(
            CalcTools.is_zero(contrarian['total_operating_cost']), 0,
            contrarian['financial_expense'] / contrarian['total_operating_cost']
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 营业费用与营业总收入之比=销售费用(TTM)/营业总收入(TTM)
    def operating_expense_rate_ttm(self, tp_contrarian, factor_contrarian):
        columns_list = ['sale_expense', 'total_operating_cost', 'total_operating_revenue']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['OperExpTTM'] = np.where(
            CalcTools.is_zero(contrarian['total_operating_cost']), 0,
            contrarian['sale_expense'] / contrarian['total_operating_revenue']
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 管理费用与营业总收入之比=管理费用/营业总收入
    def admini_expense_rate_ttm(self, tp_contrarian, factor_contrarian):
        columns_list = ['administration_expense', 'total_operating_revenue']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['AdminExpTTM'] = np.where(
            CalcTools.is_zero(contrarian['total_operating_revenue']), 0,
            contrarian['administration_expense'] / contrarian['total_operating_revenue']
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 销售期间费用率 = (财务费用 + 销售费用 + 管理费用) / (营业收入)
    def period_costs_rate_ttm(self, tp_contrarian, factor_contrarian):
        columns_list = ['financial_expense', 'sale_expense', 'administration_expense', 'operating_revenue']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['PeridCostTTM'] = np.where(
            CalcTools.is_zero(contrarian['operating_revenue']), 0,
            (contrarian['financial_expense'] + contrarian['sale_expense'] + contrarian['administration_expense']) / \
            contrarian['operating_revenue']
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 负债合计/有形资产(流动资产+固定资产)
    def debt_tangible_equity_ratio_latest(self, tp_contrarian, factor_contrarian):
        columns_list = ['total_liability', 'total_current_liability', 'fixed_assets']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['DTE'] = np.where(
            CalcTools.is_zero(contrarian['total_current_liability'] + contrarian['fixed_assets']), 0,
            contrarian['total_current_liability'] / (contrarian['total_current_liability'] + contrarian['fixed_assets'])
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # 债务总资产比=负债合计/资产合计
    def debts_asset_ratio_latest(self, tp_contrarian, factor_contrarian):
        columns_list = ['total_liability', 'total_assets']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['DA'] = np.where(
            CalcTools.is_zero(contrarian['total_assets']), 0,
            contrarian['total_liability'] / contrarian['total_assets'])
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian

    # InteBearDebtToTotalCapital = 有息负债/总资本   总资本=固定资产+净运营资本  净运营资本=流动资产-流动负债
    # InteBearDebtToTotalCapital = 有息负债/(固定资产 + 流动资产 - 流动负债)
    def inte_bear_debt_to_total_capital_latest(self, tp_contrarian, factor_contrarian):
        columns_list = ['interest_bearing_liability', 'fixed_assets', 'total_current_assets',
                        'total_current_liability']
        contrarian = tp_contrarian.loc[:, columns_list]
        contrarian['IntBDToCap'] = np.where(
            CalcTools.is_zero(contrarian['fixed_assets'] + contrarian['total_current_assets'] + \
                              contrarian['total_current_liability']), 0,
            contrarian['interest_bearing_liability'] / (contrarian['fixed_assets'] + \
                                                        contrarian['total_current_assets'] + contrarian[
                                                            'total_current_liability'])
        )
        contrarian = contrarian.drop(columns_list, axis=1)
        factor_contrarian = pd.merge(factor_contrarian, contrarian, on="symbol")
        return factor_contrarian


def calculate(trade_date, constrain_sets, constrain):  # 计算对应因子
    print(trade_date)
    factor_contrarian = pd.DataFrame()
    tp_contrarian = constrain_sets
    factor_contrarian['symbol'] = tp_contrarian['symbol']
    tp_contrarian.set_index('symbol', inplace=True)
    factor_contrarian = constrain.inte_bear_debt_to_total_capital_latest(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.debts_asset_ratio_latest(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.debt_tangible_equity_ratio_latest(constrain_sets, factor_contrarian)

    factor_contrarian = constrain.sales_cost_ratio_ttm(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.tax_ratio_ttm(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.financial_expense_rate_ttm(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.operating_expense_rate_ttm(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.admini_expense_rate_ttm(constrain_sets, factor_contrarian)
    factor_contrarian = constrain.period_costs_rate_ttm(constrain_sets, factor_contrarian)
    factor_contrarian['id'] = factor_contrarian['symbol'] + str(trade_date)
    factor_contrarian['trade_date'] = str(trade_date)
    constrain._storage_data(factor_contrarian, trade_date)




@app.task()
def factor_calculate(**kwargs):
    print("constrain_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    constrain = FactorContrarian('factor_constrain')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session, date_index)
    total_constrain_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_constrain_data {}".format(len(total_constrain_data)))
    calculate(date_index, total_constrain_data, constrain)