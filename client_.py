import pdb
from alphamind.api import *
from PyFin.api import *
from PyFin.api import makeSchedule
from sqlalchemy import create_engine, select, and_, or_
from sqlalchemy.pool import NullPool
from factors.models import Alpha191
import pandas as pd
import time
import datetime
import json
import sys
from factors import analysis
from ultron.cluster.invoke.cache_data import cache_data
from ultron.utilities.short_uuid import unique_machine,decode
    
def fetch_factor(engine191, factor_names, start_date, end_date):
    db_columns = []
    db_columns.append(Alpha191.trade_date)
    db_columns.append(Alpha191.code)
    for factor_name in factor_names:
        db_columns.append(Alpha191.__dict__[factor_name])
    query = select(db_columns).where(
        and_(Alpha191.trade_date >= start_date, Alpha191.trade_date <= end_date, ))
    return pd.read_sql(query, engine191)

def factor_combination(engine, factors, universe_name_list, start_date, end_date, freq):
    universe = None
    for name in universe_name_list:
        if universe is None:
            universe = Universe(name)
        else:
            universe += Universe(name)
    dates = makeSchedule(start_date, end_date, freq, calendar='china.sse')
    factor_negMkt = engine.fetch_factor_range(universe, "negMarketValue", dates=dates)
    risk_cov, risk_factors = engine.fetch_risk_model_range(universe, dates=dates)
    dx_returns = engine.fetch_dx_return_range(universe, dates=dates, horizon=map_freq(freq))

    # data combination
    total_data = pd.merge(factors, risk_factors, on=['trade_date', 'code'])
    total_data = pd.merge(total_data, factor_negMkt, on=['trade_date', 'code'])
    total_data = pd.merge(total_data, dx_returns, on=['trade_date', 'code'])
    
    industry_category = engine.fetch_industry_range(universe, dates=dates)
    total_data = pd.merge(total_data, industry_category, on=['trade_date', 'code']).dropna()
    total_data.dropna(inplace=True)
    return total_data

def fetch_factor_sets(**kwargs):
    db_info = kwargs["db_info"]
    factor_names = kwargs["factor_names"]
    start_date = kwargs['start_date']
    end_date = kwargs['end_date']
    universe_name_list = kwargs['universe_name']
    benchmark_code = kwargs['benchmark_code']
    freq = kwargs['freq']
  
    engine = SqlEngine(db_info) # alpha-mind engine
    engine191 = create_engine(db_info, poolclass=NullPool)
    factors = fetch_factor(engine191, factor_names, start_date, end_date)
    total_data = factor_combination(engine, factors, universe_name_list, start_date, end_date, freq)
    return total_data



#session = str('15609986886946081')
session = str(int(time.time() * 1000000 + datetime.datetime.now().microsecond))
alpha_list = []
for i in range(31,32):
    alpha_name = 'alpha_' + str(i)
    alpha_list.append(alpha_name)


db_info = 'postgresql+psycopg2://alpha:alpha@180.166.26.82:8889/alpha'

total_data = fetch_factor_sets(db_info=db_info,
                               factor_names=alpha_list, risk_styles=["SIZE"],
                               start_date='2010-01-01', end_date='2018-12-31',
                               universe_name=['zz500','hs300','ashare'],
                               benchmark_code=905,
                               freq='3b')
try:
    diff_sets = set(total_data.columns) - set(alpha_list)
except:
    import pdb
    pdb.set_trace()


grouped_list = []
for alpha_name in alpha_list:
    print(alpha_name, session)
    #pdb.set_trace()
    #print(cache_data.get_cache(session, alpha_name))
    factors_list = list(diff_sets)
    factors_list.append(alpha_name)
    factors_sets = total_data[factors_list]

    cache_data.set_cache(session, alpha_name, factors_sets.to_json(orient='records'))

    analysis.factor_analysis(factor_name=alpha_name,risk_styles=['SIZE'],
                             benchmark_code=905,
                             session=session)

