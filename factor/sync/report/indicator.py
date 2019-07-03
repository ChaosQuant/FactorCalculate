#!/usr/bin/env python
# coding=utf-8
import collections
import numpy as np
import pdb
from datetime import datetime
import sqlalchemy as sa
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import argparse
from sync_fundamentals import SyncFundamentals


class SyncIndicator(object):
    def __init__(self):
        self._sync_fun = SyncFundamentals(sa.create_engine("mssql+pymssql://read:read@192.168.100.64:1433/QADB"),
                                          None, 'indicator')

    def create_dest_tables(self):
        self._sync_fun.create_dest_tables('indicator')

    def create_dest_report_tables(self):
        self._sync_fun.create_dest_report_tables('indicator_report')

    def create_columns(self):
        columns_list = collections.OrderedDict()
        columns_list["eps"] = "decimal(19,4)"
        columns_list["adjusted_profit"] = "decimal(19,4)"
        columns_list["operating_profit"] = "decimal(19,4)"
        columns_list["value_change_profit"] = "decimal(19,4)"
        columns_list["roe"] = "decimal(19,4)"
        columns_list["inc_return"] = "decimal(19,4)"
        columns_list["roa"] = "decimal(19,4)"
        columns_list["net_profit_margin"] = "decimal(19,4)"
        columns_list["gross_profit_margin"] = "decimal(19,4)"
        columns_list["expense_to_total_revenue"] = "decimal(19,4)"
        columns_list["operation_profit_to_total_revenue"] = "decimal(19,4)"
        columns_list["net_profit_to_total_revenue"] = "decimal(19,4)"
        columns_list["operating_expense_to_total_revenue"] = "decimal(19,4)"
        columns_list["ga_expense_to_total_revenue"] = "decimal(19,4)"
        columns_list["financing_expense_to_total_revenue"] = "decimal(19,4)"
        columns_list["operating_profit_to_profit"] = "decimal(19,4)"
        columns_list["invesment_profit_to_profit"] = "decimal(19,4)"
        columns_list["adjusted_profit_to_profit"] = "decimal(19,4)"
        columns_list["goods_sale_and_service_to_revenue"] = "decimal(19,4)"
        columns_list["ocf_to_revenue"] = "decimal(19,4)"
        columns_list["ocf_to_operating_profit"] = "decimal(19,4)"
        columns_list["inc_total_revenue_year_on_year"] = "decimal(19,4)"
        columns_list["inc_total_revenue_annual"] = "decimal(19,4)"
        columns_list["inc_revenue_year_on_year"] = "decimal(19,4)"
        columns_list["inc_revenue_annual"] = "decimal(19,4)"
        columns_list["inc_operation_profit_year_on_year"] = "decimal(19,4)"
        columns_list["inc_operation_profit_annual"] = "decimal(19,4)"
        columns_list["inc_net_profit_year_on_year"] = "decimal(19,4)"
        columns_list["inc_net_profit_annual"] = "decimal(19,4)"
        columns_list["inc_net_profit_to_shareholders_year_on_year"] = "decimal(19,4)"
        columns_list["inc_net_profit_to_shareholders_annual"] = "decimal(19,4)"

        columns_list = collections.OrderedDict(sorted(columns_list.items(), key=lambda t: t[0]))
        time_columns = 'a.ENDDATE'
        del_columns = ['code', 'EXCHANGE', 'SType', 'year']
        sub_columns = ['eps',
                       'adjusted_profit',
                       'operating_profit',
                       'value_change_profit'
                       ]  # 需要拿出来算单季
        self._sync_fun.set_columns(columns_list, self.create_sql(), time_columns, del_columns, sub_columns)
        self._sync_fun.set_change_symbol(self.change_symbol)

    def create_sql(self):
        sql = """select S.Symbol AS code,S.Exchange AS EXCHANGE, S.SType,a.REPORTYEAR as year, 
                    a.FIRSTPUBLISHDATE as pub_date,
                    a.ENDDATE AS report_date,
                    a.EPSBASIC as eps,
                    a.NPCUT as adjusted_profit,
                    b.NOPI as operating_profit,
                    b.NVALCHGIT as value_change_profit,
                    a.ROEDILUTED as roe,
                    b.ROEDILUTEDCUT as inc_return,
                    b.ROAAANNUAL as roa,
                    b.SNPMARGINCONMS as net_profit_margin,
                    b.SGPMARGIN as gross_profit_margin,
                    c.OCOI as expense_to_total_revenue,
                    b.OPPRORT as operation_profit_to_total_revenue,
                    c.PROFITRATIO as net_profit_to_total_revenue,
                    b.OPEXPRT as operating_expense_to_total_revenue,
                    b.MGTEXPRT as ga_expense_to_total_revenue,
                    b.FINLEXPRT as financing_expense_to_total_revenue,
                    b.OPANITOTP as operating_profit_to_profit,
                    b.NVALCHGITOTP as invesment_profit_to_profit,
                    b.ROEDILUTEDCUT as adjusted_profit_to_profit,
                    b.SCASHREVTOOPIRT as goods_sale_and_service_to_revenue,
                    b.OPANCFTOOPNI as ocf_to_operating_profit,
                    b.TAGRT as inc_total_revenue_year_on_year,
                    c.OPERINYOYB as inc_revenue_year_on_year,
                    c.OPERPROYOYB as inc_operation_profit_year_on_year,
                    c.NETPROFITYOYB as inc_net_profit_year_on_year,
                    c.NETINPNRPLYOYB as inc_net_profit_to_shareholders_year_on_year
                    from TQ_FIN_PROFINMAININDEX a
                    left join TQ_FIN_PROINDICDATA b
                        on a.COMPCODE=b.COMPCODE and a.REPORTYEAR=b.REPORTYEAR and  b.REPORTTYPE=3 and a.REPORTDATETYPE=b.REPORTDATETYPE
                    left join TQ_FIN_PROINDICDATASUB c
                        on a.COMPCODE=c.COMPCODE and a.REPORTYEAR=c.REPORTYEAR and  c.REPORTTYPE=3 and a.REPORTDATETYPE=c.REPORTDATETYPE
                    JOIN FCDB.dbo.SecurityCode as S
                             on S.CompanyCode = a.COMPCODE
                    where a.REPORTTYPE={0} AND S.SType='{1}' and S.Enabled=0 and S.Status=0  AND """.format(1,
                                                                                                            'EQA')
        return sql

    def change_symbol(self, trades_date_df):
        return np.where(trades_date_df['EXCHANGE'] == 'CNSESH',
                        trades_date_df['code'] + '.XSHG',
                        trades_date_df['code'] + '.XSHE')

    def update_report(self, start_date, end_date, count):
        self._sync_fun.update_report(start_date, end_date, count)

    def do_update(self, start_date, end_date, count, order='DESC'):
        self._sync_fun.do_update(start_date, end_date, count, order)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=int, default=20070101)
    parser.add_argument('--end_date', type=int, default=0)
    parser.add_argument('--count', type=int, default=2)
    parser.add_argument('--rebuild', type=bool, default=False)
    parser.add_argument('--update', type=bool, default=False)
    parser.add_argument('--report', type=bool, default=False)
    parser.add_argument('--rebuild_report', type=bool, default=False)
    parser.add_argument('--schedule', type=bool, default=False)
    args = parser.parse_args()
    if args.end_date == 0:
        end_date = int(datetime.now().date().strftime('%Y%m%d'))
    else:
        end_date = args.end_date
    if args.rebuild:
        processor = SyncIndicator()
        processor.create_columns()
        processor.create_dest_tables()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.update:
        processor = SyncIndicator()
        processor.create_columns()
        processor.do_update(args.start_date, end_date, args.count)
    elif args.rebuild_report:
        processor = SyncIndicator()
        processor.create_columns()
        processor.create_dest_report_tables()
        processor.update_report(args.start_date, end_date, args.count)
    elif args.report:
        processor = SyncIndicator()
        processor.create_columns()
        processor.update_report(args.start_date, end_date, args.count)
    if args.schedule:
        processor = SyncIndicator()
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name, 'trade_date')
        print('running schedule task, start date:', start_date, ';end date:', end_date)
        processor.do_update(start_date, end_date, -1, '')
        processor.create_columns()
        start_date = processor._sync_fun.get_start_date(processor._sync_fun._table_name + '_report', 'report_date')
        print('running schedule report task, start date:', start_date, ';end date:', end_date)
        processor.update_report(start_date, end_date, -1)
