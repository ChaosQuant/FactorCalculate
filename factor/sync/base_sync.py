import sys

from tm_import_utils import TmImportUtils

sys.path.append('..')

import config
import sqlalchemy as sa
import pandas as pd
from sqlalchemy.orm import sessionmaker


class BaseSync(object):
    def __init__(self, dest_table):
        source_db = '''mssql+pymssql://{0}:{1}@{2}:{3}/{4}'''.format(config.source_db_user, config.source_db_pwd,
                                                                     config.source_db_host, config.source_db_port,
                                                                     config.source_db_database)
        destination_db = '''mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'''.format(config.destination_db_user,
                                                                                 config.destination_db_pwd,
                                                                                 config.destination_db_host,
                                                                                 config.destination_db_port,
                                                                                 config.destination_db_database)
        # 源数据库
        self.source = sa.create_engine(source_db)
        # 目标数据库
        self.destination = sa.create_engine(destination_db)
        # 目标数据库Session
        self.dest_session = sessionmaker(bind=self.destination, autocommit=False, autoflush=True)
        self.dest_table = dest_table

    def get_start_date(self):
        sql = """select max(trade_date) as trade_date from `{0}`;""".format(self.dest_table)
        trades_sets = pd.read_sql(sql, self.destination)
        td = 20070101
        if not trades_sets.empty:
            td = trades_sets['trade_date'][0]
            td = str(td).replace('-', '')
        return td

    def delete_trade_data(self, trade_date):
        session = self.dest_session()
        session.execute('''delete from `{0}` where trade_date={1}'''.format(self.dest_table, trade_date))
        session.commit()

    def create_table(self, create_sql):
        drop_sql = """drop table if exists `{0}`;""".format(self.dest_table)
        session = self.dest_session()
        session.execute(drop_sql)
        session.execute(create_sql)
        session.execute(
            '''alter table `{0}` add  `creat_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP;'''.format(self.dest_table))
        session.execute(
            '''alter table `{0}` add  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;'''.format(
                self.dest_table))
        session.commit()
        session.close()

    def insert_or_update(self, datas):
        session = self.dest_session()
        for i in range(datas.shape[0]):
            data = datas.iloc[i]
            values = ''
            update = ''
            title = ''
            for j in range(len(data)):
                index = data.index[j]
                value = str(data[j]).replace("'", "\\'").replace("%", "\\%")
                title += """`{0}`,""".format(index)
                values += """'{0}',""".format(value)
                update += """`{0}`='{1}',""".format(index, value)

            sql = '''insert into {0} ({1}) values({2}) ON DUPLICATE KEY UPDATE {3}'''.format(self.dest_table,
                                                                                             title[0:-1],
                                                                                             values[0:-1],
                                                                                             update[0:-1]
                                                                                             )
            sql = sql.replace("'nan'", 'Null').replace("'None'", 'Null')
            session.execute(sql)
        session.commit()
        session.close()
