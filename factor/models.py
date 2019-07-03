# -*- coding: utf-8 -*-
from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, String, Text, Boolean, text, JSON,TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Growth(Base):
    __tablename__ = 'growth'
    trade_date = Column(DateTime, primary_key=True, nullable=False)
    code = Column(Integer, primary_key=True, nullable=False)
    net_asset_grow_rate_latest = Column(Float(53))
    total_asset_grow_rate_latest = Column(Float(53))
    operating_revenue_grow_rate_ttm = Column(Float(53))
    operating_profit_grow_rate_ttm = Column(Float(53))
    total_profit_grow_rate_ttm = Column(Float(53))
    net_profit_grow_rate_ttm = Column(Float(53))
    np_parent_company_grow_rate = Column(Float(53))
    net_profit_grow_rate_3y_ttm = Column(Float(53))
    net_profit_grow_rate_5y_ttm = Column(Float(53))
    operating_revenue_grow_rate_3y_ttm = Column(Float(53))
    operating_revenue_grow_rate_5y_ttm = Column(Float(53))
    net_cash_flow_grow_rate_ttm = Column(Float(53))
    np_parent_company_cut_yoy_ttm = Column(Float(53))
    growth_egro_ttm = Column(Float(53))
    growth_sue_ttm = Column(Float(53))
    growth_suoi_ttm = Column(Float(53))
    financing_cash_grow_rate_ttm = Column(Float(53))
    invest_cash_grow_rate_ttm = Column(Float(53))
    oper_cash_grow_rate_ttm = Column(Float(53))
    growth_sgro_ttm = Column(Float(53))
