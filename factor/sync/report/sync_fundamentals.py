#!/usr/bin/env python
# coding=utf-8
import os
import sys
from datetime import datetime
import sqlalchemy as sa
import pandas as pd
import numpy as np
import collections

from sqlalchemy.orm import sessionmaker
import multiprocessing

sys.path.append('../..')
from utillities.sync_util import SyncUtil
from utillities.calc_tools import CalcTools
import config
from utillities.time_common import TimeCommon


class SyncFundamentals(object):

    def __init__(self, source=None, destination=None, table_name=''):
        source_db = '''mssql+pymssql://{0}:{1}@{2}:{3}/{4}'''.format(config.source_db_user, config.source_db_pwd,
                                                                     config.source_db_host, config.source_db_port,
                                                                     config.source_db_database)
        destination_db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.destination_db_user,
                                                                                 config.destination_db_pwd,
                                                                                 config.destination_db_host,
                                                                                 config.destination_db_port,
                                                                                 config.destination_db_database)
        self._sync_util = SyncUtil()
        if source == None:
            self._source = sa.create_engine(source_db)
        else:
            self._source = source
        # self._destination = destination
        self._destination = sa.create_engine(destination_db)
        self._dest_session = sessionmaker(bind=self._destination, autocommit=False, autoflush=True)
        self._dir = config.RECORD_BASE_DIR
        self._secondary_func = None
        self._table_name = table_name

    def get_start_date(self, table, type='trade_date'):
        sql = """select max({0}) as trade_date from `{1}`;""".format(type, table)
        trades_sets = pd.read_sql(sql, self._destination)
        td = 20070101
        if not trades_sets.empty:
            td = trades_sets['trade_date'][0]
            td = str(td).replace('-', '')
        return td

    def set_columns(self, columns, get_sql, year_columns, del_columns,
                    sub_columns):  # key和 value 类型
        self._columns = columns
        self._get_sql = get_sql
        self._time_columns = year_columns
        self._del_columns = del_columns
        self._sub_columns = sub_columns

    # 二次加工处理
    def secondary_sets(self, secondary_func):
        self._secondary_func = secondary_func

    def set_change_symbol(self, change_symbol):
        self._change_symbol = change_symbol

    def create_index(self):
        session = self._dest_session()
        indexs = [
            '''CREATE INDEX {0}_trade_date_symbol_index ON `{0}` (trade_date, symbol);'''.format(self._table_name)
        ]
        for sql in indexs:
            session.execute(sql)
        session.commit()
        session.close()

    def create_dest_tables(self, table_name):
        drop_sql = """drop table if exists `{0}`;""".format(table_name)
        create_sql = """create table `{0}`(
            `id` varchar(32) NOT NULL,
            `symbol` varchar(24) NOT NULL,
            `pub_date` date NOT NULL,
            `trade_date` date NOT NULL,
            `report_date`  date NOT NULL,""".format(table_name)
        for key, value in self._columns.items():
            create_sql += """`{0}` {1}  DEFAULT NULL,""".format(key, value)

        create_sql += "PRIMARY KEY(`id`,`symbol`,`trade_date`,`report_date`)"
        create_sql += """) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        session = self._dest_session()
        session.execute(drop_sql)
        session.execute(create_sql)
        session.execute(
            '''alter table `{0}` add  `creat_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP;'''.format(self._table_name))
        session.execute(
            '''alter table `{0}` add  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;'''.format(
                self._table_name))
        session.commit()
        session.close()
        self.create_index()

    def create_dest_report_tables(self, table_name):
        drop_sql = """drop table if exists `{0}`;""".format(table_name)
        create_sql = """create table `{0}`(
            `symbol` varchar(24) NOT NULL,
            `pub_date` date NOT NULL,
            `report_date`  date NOT NULL,""".format(table_name)
        for key, value in self._columns.items():
            create_sql += """`{0}` {1}  DEFAULT NULL,""".format(key, value)

        create_sql += "PRIMARY KEY(`symbol`,`report_date`,`pub_date`)"
        create_sql += """) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
        session = self._dest_session()
        session.execute(drop_sql)
        session.execute(create_sql)
        session.commit()
        session.close()

    def year_update(self, df):
        df.loc[df.index, self._sub_columns] = df[self._sub_columns] - df[self._sub_columns].shift(-1).fillna(0)
        return df

    def change_single(self, year):
        fundamentals_sets_year = self.fundamentals_sets[self.fundamentals_sets['year'] == year]
        stock_list = list(set(fundamentals_sets_year['code']))
        new_fundamentals_sets = pd.DataFrame()
        for stock in stock_list:
            print(stock)
            new_fundamentals_sets = new_fundamentals_sets.append(self.year_update(
                fundamentals_sets_year[
                    fundamentals_sets_year['code'] == stock]))
        return new_fundamentals_sets

    def foo(self, x):
        sum = 0
        for i in range(x):
            sum = i
        return sum

    def fetch_batch_fundamentals(self, report_start_date):
        self._get_sql += """ {0} >= '{1}' ORDER BY {0} DESC;""".format(self._time_columns,
                                                                       report_start_date)
        fundamentals_sets = pd.read_sql(self._get_sql.format(report_start_date), self._source)
        # fundamentals_sets = fundamentals_sets.apply(self._unit.plus_year, axis=1)
        fundamentals_sets = fundamentals_sets.fillna(0)
        # return fundamentals_sets
        # 读取年
        # 读取股票代码
        new_fundamentals_sets = pd.DataFrame()
        year_list = list(set(fundamentals_sets['year']))
        year_list.sort(reverse=True)
        stock_list = list(set(fundamentals_sets['code']))
        params = []
        cpus = multiprocessing.cpu_count()
        if len(year_list) < 4:
            for i in range(cpus):
                stock_list_cpu = stock_list[i::cpus]
                params.append({
                    'fundamentals_sets_symbol': fundamentals_sets[fundamentals_sets['code'].isin(stock_list_cpu)],
                    'sub_columns': self._sub_columns,
                    'cpu': i
                })
            with multiprocessing.Pool(processes=cpus) as p:
                res = p.map(CalcTools.change_single_by_symbol, params)
        else:
            for year in year_list:
                print(year)
                params.append({
                    'fundamentals_sets_year': fundamentals_sets[fundamentals_sets['year'] == year],
                    'sub_columns': self._sub_columns,
                    'year': year
                })
                # fundamentals_sets_year = fundamentals_sets[fundamentals_sets['year'] == year]
                # for stock in stock_list:
                #     new_fundamentals_sets = new_fundamentals_sets.append(self.year_update(
                #         fundamentals_sets_year[
                #             fundamentals_sets_year['code'] == stock]))
            # with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as p:
            #     res = p.map(self.foo, years)
            with multiprocessing.Pool(processes=cpus) as p:
                res = p.map(CalcTools.change_single, params)
        new_fundamentals_sets = pd.concat(res, sort=False, axis=0)
        return new_fundamentals_sets

    def update_fundamentals(self, trades_sets, fundamentals_sets, report_date_list):
        session = self._dest_session()
        trades_list = list(trades_sets['TRADEDATE'])
        trades_list.sort(reverse=True)
        for trade_date in trades_list:
            print(trade_date)
            date_range = self._sync_util.every_report_range(trade_date, report_date_list)
            trades_date_fundamentals = fundamentals_sets[(fundamentals_sets['report_date'] >= str(date_range[1])) & (
                    fundamentals_sets['pub_date'] <= str(date_range[0]))]
            trades_date_fundamentals.sort_values(by='report_date', ascending=False, inplace=True)
            trades_date_fundamentals.drop_duplicates(subset=['code'], keep='first', inplace=True)
            trades_date_fundamentals['trade_date'] = trade_date
            trades_date_fundamentals = trades_date_fundamentals.dropna(how='all')
            trades_date_fundamentals['symbol'] = self._change_symbol(trades_date_fundamentals)
            trades_date_fundamentals['id'] = trades_date_fundamentals['symbol'] + trades_date_fundamentals['trade_date']
            trades_date_fundamentals.drop(self._del_columns, axis=1, inplace=True)
            # 二次加工
            if self._secondary_func is not None:
                trades_date_fundamentals = self._secondary_func(trades_date_fundamentals)
            # 本地保存
            file_path = self._dir + '/' + self._table_name
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            file_name = os.path.join(file_path, str(trade_date)) + '.csv'
            if os.path.exists(str(file_name)):
                os.remove(str(file_name))
            trades_date_fundamentals.reset_index(drop=True, inplace=True)
            trades_date_fundamentals.to_csv(file_name, encoding='UTF-8')
            try:
                session.execute('''delete from `{0}` where trade_date={1}'''.format(self._table_name, trade_date))
                session.commit()
                trades_date_fundamentals.to_sql(name=self._table_name, con=self._destination, if_exists='append',
                                                index=False)
            except Exception as e:
                print(e.orig.msg)
                self.insert_or_update(trades_date_fundamentals)

    def update_report(self, start_date, end_date, count):
        session = self._dest_session()
        trade_sets = self._sync_util.get_trades_ago('001002', start_date, end_date, count)
        min_year = int(int(trade_sets['TRADEDATE'].min()) / 10000)
        max_year = int(int(trade_sets['TRADEDATE'].max()) / 10000)
        report_date_list = self._sync_util.create_report_date(min_year, max_year)
        min_report_year = report_date_list[0]
        new_fundamentals = self.fetch_batch_fundamentals(min_report_year)
        for report_date in report_date_list:
            report_fundamentals = new_fundamentals[
                # new_fundamentals['report_date'] == datetime.strptime(str(report_date), '%Y%m%d')]
                new_fundamentals['report_date'] == str(report_date)]
            if report_fundamentals.empty:
                continue
            report_fundamentals['symbol'] = self._change_symbol(report_fundamentals)
            report_fundamentals.drop(self._del_columns, axis=1, inplace=True)

            # 二次加工
            if self._secondary_func is not None:
                report_fundamentals = self._secondary_func(report_fundamentals)
            file_path = self._dir + '/report/' + self._table_name
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            file_name = os.path.join(file_path, str(report_date)) + '.csv'
            if os.path.exists(str(file_name)):
                os.remove(str(file_name))
            report_fundamentals.to_csv(self._dir + '/report/' + self._table_name + '/' + str(report_date) + '.csv',
                                       encoding='UTF-8')

            try:
                session.execute('''delete from `{0}` where report_date={1}'''.format(self._table_name, report_date))
                session.commit()
                report_fundamentals.to_sql(name=self._table_name + '_report', con=self._destination, if_exists='append',
                                           index=False)
            except Exception as e:
                print(e.orig.msg)
                self.insert_or_update(report_fundamentals)

    def do_update(self, start_date, end_date, count, order):
        trade_sets = self._sync_util.get_trades_ago('001002', start_date, end_date, count, order)
        min_year = int(int(trade_sets['TRADEDATE'].min()) / 10000)
        max_year = int(int(trade_sets['TRADEDATE'].max()) / 10000)
        report_date_list = self._sync_util.create_report_date(min_year, max_year)
        # 将初始放入进去获取完整一年报告，因此用报告最早期读取数据
        min_report_year = report_date_list[0]
        new_fundamentals = self.fetch_batch_fundamentals(min_report_year)
        self.update_fundamentals(trade_sets, new_fundamentals, report_date_list)

    def insert_or_update(self, datas):
        session = self._dest_session()
        for i in range(datas.shape[0]):
            data = datas.iloc[i]
            values = ''
            update = ''
            title = ''
            for j in range(len(data)):
                index = data.index[j]
                value = str(data[j]).replace("'", "\\'")
                title += """`{0}`,""".format(index)
                values += """'{0}',""".format(value)
                update += """`{0}`='{1}',""".format(index, value)

            sql = '''insert into {0} ({1}) values({2}) ON DUPLICATE KEY UPDATE {3}'''.format(self._table_name,
                                                                                             title[0:-1],
                                                                                             values[0:-1],
                                                                                             update[0:-1]
                                                                                             )
            sql = sql.replace("'nan'", 'Null').replace("'None'", 'Null')
            session.execute(sql)
        session.commit()
        session.close()


def change_symbol(trades_date_df):
    return np.where(trades_date_df['EXCHANGE'] == 'CNSESH',
                    trades_date_df['code'] + '.XSHG',
                    trades_date_df['code'] + '.XSHE')


if __name__ == '__main__':
    sync_fun = SyncFundamentals(sa.create_engine("mssql+pymssql://read:read@10.17.205.155:1433/FCDB"),
                                sa.create_engine("mysql+mysqlconnector://root:t2R7P7@10.15.5.86:3306/factors"))
    columns_list = collections.OrderedDict()
    columns_list["total_operating_revenue"] = "decimal(19,4)"  # 9
    columns_list["total_operating_cost"] = "decimal(19,4)"  # 8
    columns_list["operating_revenue"] = "decimal(19,4)"  # 5
    columns_list["operating_cost"] = "decimal(19,4)"  # 4
    columns_list["interest_income"] = "decimal(19,4)"  # 1
    columns_list["permiums_earned"] = "decimal(19,4)"  # 6
    columns_list["commission_income"] = "decimal(19,4)"  # 0
    columns_list["refunded_premiums"] = "decimal(19,4)"  # 7
    columns_list["net_pay_insurance_claims"] = "decimal(19,4)"  # 2
    columns_list["withdraw_insurance_contract_reserve"] = "decimal(19,4)"  # 10
    columns_list["net_profit"] = "decimal(19,4)"  # 3
    columns_list = collections.OrderedDict(sorted(columns_list.items(), key=lambda t: t[0]))
    sql = """select S.Symbol AS code,S.Exchange AS EXCHANGE, S.SType, C.PublishDate AS pub_date,
            C.ReportDate AS report_date,C.ReportStyle,
            C.CINST61 AS {0},
            C.CINST65 AS {1},
            C.CINST1 AS {2},
            C.CINST3 AS {3},
            C.CINST62 AS {4},
            C.CINST64 AS {5},
            C.CINST68 AS {6},
            C.CINST63 AS {7},
            C.CINST69 AS {8},
            C.CINST70 AS {9},
            C.CINST24 AS {10}
            from FCDB.dbo.CINST_New AS C JOIN FCDB.dbo.SecurityCode as S ON S.CompanyCode = C.CompanyCode
            where C.ReportStyle=11 AND S.SType='{11}' AND  S.Symbol in ('600519')  AND """.format(
        columns_list.keys()[9],
        columns_list.keys()[8],
        columns_list.keys()[5],
        columns_list.keys()[4],
        columns_list.keys()[1],
        columns_list.keys()[6],
        columns_list.keys()[0],
        columns_list.keys()[7],
        columns_list.keys()[2],
        columns_list.keys()[10],
        columns_list.keys()[3],
        'EQA')
    time_columns = 'C.ReportDate'
    del_columns = ['code', 'EXCHANGE', 'SType', 'ReportStyle', 'year', 'report_type']
    sync_fun.set_columns(columns_list, sql, time_columns, del_columns)
    sync_fun.set_change_symbol(change_symbol)
    sync_fun.create_dest_tables('income')
    sync_fun.do_update(20, '20190107')
