#!/usr/bin/env python
# coding=utf-8

import numpy as np

import json
from factor import app
from pandas.io.json import json_normalize

from factor.ttm_fundamental import *
from factor.factor_base import FactorBase
from vision.fm.signletion_engine import *
from vision.utillities.calc_tools import CalcTools
from ultron.cluster.invoke.cache_data import cache_data



class FactorCashFlow(FactorBase):
    def __init__(self, name):
        super(FactorCashFlow, self).__init__(name)

    # 构建因子表
    def create_dest_tables(self):
        drop_sql = """drop table if exists `{0}`""".format(self._name)
        create_sql = """create table `{0}`(
                    `id` varchar(32) NOT NULL,
                    `symbol` varchar(24) NOT NULL,
                    `trade_date` date NOT NULL,
                    `nocf_to_t_liability_ttm` decimal(19,4),
                    `nocf_to_interest_bear_debt_ttm` decimal(19,4),
                    `nocf_to_net_debt_ttm` decimal(19,4),
                    `sale_service_cash_to_or_ttm` decimal(19,4),
                    `cash_rate_of_sales_ttm` decimal(19,4),
                    `nocf_to_operating_ni_ttm` decimal(19,4),
                    `oper_cash_in_to_current_liability_ttm` decimal(19,4),
                    `cash_to_current_liability_ttm` decimal(19,4),
                    `cfo_to_ev_ttm` decimal(19,4),
                    `acca_ttm` decimal(19,4),
                    `net_profit_cash_cover_ttm` decimal(19,4),
                    `oper_cash_in_to_asset_ttm` decimal(19,4),
                    `sales_service_cash_to_or_latest` decimal(19,4),
                    `cash_rate_of_sales_latest` decimal(19,4),
                    `nocf_to_operating_ni_latest` decimal(19,4),
                    PRIMARY KEY(`id`,`trade_date`,`symbol`)
                    )ENGINE=InnoDB DEFAULT CHARSET=utf8;""".format(self._name)
        super(FactorCashFlow, self)._create_tables(create_sql, drop_sql)

    # 经营活动净现金流（TTM）/负债（TTM）
    def nocf_to_t_liability_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'total_liability']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['nocf_to_t_liability_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.total_liability.values), 0,
            cash_flow.net_operate_cash_flow_ttm.values / cash_flow.total_liability.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动净现金流（TTM）/带息负债（TTM）
    def nocf_to_interest_bear_debt_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'total_liability', 'interest_bearing_liability']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['nocf_to_interest_bear_debt_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.interest_bearing_liability.values), 0,
            cash_flow.net_operate_cash_flow_ttm.values / cash_flow.interest_bearing_liability.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动净现金流（TTM）/净负债（TTM）
    def nocf_to_net_debt_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'net_liability']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['nocf_to_net_debt_ttm'] = np.where(CalcTools.is_zero(cash_flow.net_liability.values), 0,
                                                     cash_flow.net_operate_cash_flow_ttm.values / cash_flow.net_liability.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 销售商品和提供劳务收到的现金（TTM）/营业收入（TTM）
    def sale_service_cash_to_or_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['goods_sale_and_service_render_cash_ttm', 'operating_revenue_ttm']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['sale_service_cash_to_or_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue_ttm.values), 0,
            cash_flow.goods_sale_and_service_render_cash_ttm.values / cash_flow.operating_revenue_ttm.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/营业收入（TTM）
    def cash_rate_of_sales_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'operating_revenue_ttm']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['cash_rate_of_sales_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.operating_revenue_ttm.values), 0,
            cash_flow.net_operate_cash_flow_ttm.values / cash_flow.operating_revenue_ttm.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/(营业总收入（TTM）-营业总成本（TTM）)
    def nocf_to_operating_ni_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'total_operating_revenue_ttm', 'total_operating_cost_ttm']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['nocf_to_operating_ni_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.total_operating_revenue_ttm.values - cash_flow.total_operating_cost_ttm.values),
            0, cash_flow.net_operate_cash_flow_ttm.values / (
                    cash_flow.total_operating_revenue_ttm.values - cash_flow.total_operating_cost_ttm.values))
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/流动负债（TTM）
    def oper_cash_in_to_current_liability_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'total_current_liability']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['oper_cash_in_to_current_liability_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.total_current_liability.values), 0,
            cash_flow.net_operate_cash_flow_ttm.values / cash_flow.total_current_liability.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 期末现金及现金等价物余额（TTM）/流动负债（TTM）
    def cash_to_current_liability_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['cash_and_equivalents_at_end', 'total_current_assets']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['cash_to_current_liability_ttm'] = np.where(CalcTools.is_zero(cash_flow.total_current_assets.values),
                                                              0,
                                                              cash_flow.cash_and_equivalents_at_end.values / cash_flow.total_current_assets.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/（长期借款（TTM）+ 短期借款（TTM）+ 总市值 - 期末现金及现金等价物（TTM）
    def cfo_to_ev_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'longterm_loan', 'shortterm_loan', 'market_cap',
                        'cash_and_equivalents_at_end']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['cfo_to_ev_ttm'] = np.where(CalcTools.is_zero(
            cash_flow.longterm_loan.values + cash_flow.shortterm_loan.values + \
            cash_flow.market_cap.values - cash_flow.cash_and_equivalents_at_end.values), 0,
            cash_flow.net_operate_cash_flow_ttm.values / (cash_flow.longterm_loan.values + cash_flow.shortterm_loan.values + \
                                                      cash_flow.market_cap.values - cash_flow.cash_and_equivalents_at_end.values))
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # （经营活动产生的金流量净额（TTM） - 净利润（TTM）） /总资产（TTM）
    def acca_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'net_profit', 'total_assets']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['acca_ttm'] = np.where(CalcTools.is_zero(cash_flow.total_assets.values), 0,
                                         (cash_flow.net_operate_cash_flow_ttm.values - cash_flow.net_profit.values) / (
                                             cash_flow.total_assets.values))
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/归属于母公司所有者的净利润（TTM）
    def net_profit_cash_cover_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'np_parent_company_owners']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['net_profit_cash_cover_ttm'] = np.where(
            CalcTools.is_zero(cash_flow.np_parent_company_owners.values), 0,
            cash_flow.net_operate_cash_flow_ttm.values / cash_flow.np_parent_company_owners.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（TTM）/总资产（TTM）
    def oper_cash_in_to_asset_ttm(self, ttm_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow_ttm', 'total_assets']
        cash_flow = ttm_cash_flow.loc[:, columns_list]
        cash_flow['oper_cash_in_to_asset_ttm'] = np.where(CalcTools.is_zero(cash_flow.total_assets.values),
                                                          0,
                                                          cash_flow.net_operate_cash_flow_ttm.values / cash_flow.total_assets.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 销售商品和提供劳务收到的现金（Latest）/营业收入（Latest）
    def sales_service_cash_to_or_latest(self, tp_cash_flow, factor_cash_flow):
        columns_list = ['goods_sale_and_service_render_cash', 'operating_revenue']
        cash_flow = tp_cash_flow.loc[:, columns_list]
        cash_flow['sales_service_cash_to_or_latest'] = np.where(CalcTools.is_zero(cash_flow.operating_revenue.values),
                                                                0,
                                                                cash_flow.goods_sale_and_service_render_cash.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经验活动产生的现金流量净额 / 营业收入
    def cash_rate_of_sales_latest(self, tp_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow', 'operating_revenue']
        cash_flow = tp_cash_flow.loc[:, columns_list]
        cash_flow['cash_rate_of_sales_latest'] = np.where(CalcTools.is_zero(cash_flow.operating_revenue.values),
                                                          0,
                                                          cash_flow.net_operate_cash_flow.values / cash_flow.operating_revenue.values)
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow

    # 经营活动产生的现金流量净额（Latest）/(营业总收入（Latest）-营业总成本（Latest）)
    def nocf_to_operating_ni_latest(self, tp_cash_flow, factor_cash_flow):
        columns_list = ['net_operate_cash_flow', 'total_operating_revenue',
                        'total_operating_cost']
        cash_flow = tp_cash_flow.loc[:, columns_list]
        cash_flow['nocf_to_operating_ni_latest'] = np.where(
            CalcTools.is_zero((cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values)), 0,
            cash_flow.net_operate_cash_flow.values / (
                    cash_flow.total_operating_revenue.values - cash_flow.total_operating_cost.values))
        cash_flow = cash_flow.drop(columns_list, axis=1)
        factor_cash_flow = pd.merge(factor_cash_flow, cash_flow, on="symbol")
        return factor_cash_flow


def calculate(trade_date, tp_cash_flow, cash_flow):  # 计算对应因子
    print(trade_date)
    # 读取目前涉及到的因子
    factor_cash_flow = pd.DataFrame()
    factor_cash_flow['symbol'] = tp_cash_flow['symbol']
    tp_cash_flow.set_index('symbol', inplace=True)
    
    # 非TTM计算
    factor_cash_flow = cash_flow.nocf_to_operating_ni_latest(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.cash_rate_of_sales_latest(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.sales_service_cash_to_or_latest(tp_cash_flow, factor_cash_flow)

    # TTM计算
    factor_cash_flow = cash_flow.nocf_to_t_liability_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.nocf_to_interest_bear_debt_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.nocf_to_net_debt_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.sale_service_cash_to_or_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.cash_rate_of_sales_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.nocf_to_operating_ni_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.oper_cash_in_to_current_liability_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.cash_to_current_liability_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.cfo_to_ev_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.acca_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.net_profit_cash_cover_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow = cash_flow.oper_cash_in_to_asset_ttm(tp_cash_flow, factor_cash_flow)
    factor_cash_flow['id'] = factor_cash_flow['symbol'] + str(trade_date)
    factor_cash_flow['trade_date'] = str(trade_date)
    cash_flow._storage_data(factor_cash_flow, trade_date)


def do_update(self, start_date, end_date, count):
    # 读取本地交易日
    trade_date_sets = self._trade_date.trade_date_sets_ago(start_date, end_date, count)
    for trade_date in trade_date_sets:
        self.calculate(trade_date)
    print('----->')


@app.task()
def factor_calculate(**kwargs):
    print("cash_flow_kwargs: {}".format(kwargs))
    date_index = kwargs['date_index']
    session = kwargs['session']
    cash_flow = FactorCashFlow('factor_cash_flow')  # 注意, 这里的name要与client中新建table时的name一致, 不然回报错
    content = cache_data.get_cache(session, date_index)
    total_cash_flow_data = json_normalize(json.loads(str(content, encoding='utf8')))
    print("len_total_cash_flow_data {}".format(len(total_cash_flow_data)))
    calculate(date_index, total_cash_flow_data, cash_flow)
