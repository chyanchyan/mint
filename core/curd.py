import os
from typing import List
from copy import copy
from datetime import datetime as dt
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import MetaData, text, Table, Column, Integer, String, DateTime, DECIMAL
from sqlalchemy.schema import CreateSchema, DropSchema, CreateTable, CreateColumn
from sqlalchemy.exc import ResourceClosedError

from helper_function.func import check_param_valid_range
from helper_function.file import snapshot


def print_action(func):
    def wrapper(*args, **kwargs):
        print(f'[***{str.upper(func.__name__)}***]')
        res = func(*args, **kwargs)
        return res

    return wrapper


def print_stmt(func):
    def wrapper(*args, **kwargs):
        stmt = func(*args, **kwargs)
        print_stmt_with_format(stmt=stmt)
        return stmt

    return wrapper


def print_stmt_with_format(stmt, title=''):
    if len(stmt) == 0:
        return
    title = ' sql statements to execute: ' + title
    side = "*" * int(50 - len(title) / 2)
    print(f'{side}{title}{side}')
    print(stmt)
    print('*' * 100)


class CURD:
    def __init__(self, url):
        self.url = url
        self.engine = create_engine(url=url)
        self.schema = self.engine.url.database
        self.con = self.engine.connect()
        self.metadata = MetaData()

    @staticmethod
    def get_col_stmt_str(cols: List[Column]):
        col_str = ", \n".join(
            [
                f'`{col.name}` {col.type}'
                for col in cols
            ]
        )
        return col_str

    def filter_non_exist_cols(self, row, table_name, if_non_exist_cols):
        row_ = copy(row)
        cols = self.execute(
            self.stmt_show_cols(table_name=table_name)
        )

        col_names = [col.Field for col in cols]
        non_exist_col_names = list(set(row.keys()) - set(col_names))

        if len(non_exist_col_names) > 0:
            if if_non_exist_cols == 'ignore':
                print(f'non exist cols: {non_exist_col_names}. ignore them')
                for non_exist_col in non_exist_col_names:
                    row_.pop(non_exist_col)
            elif if_non_exist_cols == 'fail':
                print(f'non exist cols: {non_exist_col_names}. exec failed')
                return {}
        return row_

    def get_constraints_info(self):
        stmt = "select * " \
               "from information_schema.KEY_COLUMN_USAGE " \
               f"where CONSTRAINT_SCHEMA ='{self.schema}'" \
               "and CONSTRAINT_NAME <> 'PRIMARY'"
        res = pd.read_sql(sql=stmt, con=self.con)
        return res

    def get_primary_info(self, table_name=None):
        stmt = "select * " \
               "from information_schema.KEY_COLUMN_USAGE " \
               f"where CONSTRAINT_SCHEMA ='{self.schema}'" \
               "and CONSTRAINT_NAME = 'PRIMARY'"

        if table_name:
            stmt = stmt + ' and TABLE_NAME = "%s"' % table_name

        res = pd.read_sql(sql=stmt, con=self.con)
        return res

    def get_cst_pki(self):
        stmt = "select * " \
               "from information_schema.KEY_COLUMN_USAGE " \
               f"where CONSTRAINT_SCHEMA ='{self.schema}'"

        res = pd.read_sql(sql=stmt, con=self.con)
        return res

    def execute(self, stmt, parameters=None):
        if len(stmt) == 0:
            print('stmt is empty')
            return None
        if isinstance(stmt, str):
            try:
                res = self.con.execute(text(stmt), parameters=parameters).fetchall()

            except ResourceClosedError as e:
                print(e)
                return None
            return res
        elif isinstance(stmt, list):
            stmts = ';\n'.join(stmt)
            self.execute(stmt=stmts, parameters=parameters)

    def snapshot_schema(self, dst_folder):
        pass

    def snapshot_table(self, table_name, folder):
        time_str = dt.now().strftime('%Y%m%d_%H%M%S_%f')

        df = pd.read_sql(sql=f'select * from `{self.schema}`.`{table_name}`', con=self.con)

        df.to_excel(os.path.join(folder, f'{table_name}_{time_str}.xlsx'))

    @check_param_valid_range(['if_exists'], [['fail', 'replace', 'skip']])
    def create_schema(self, schema, if_exists='fail'):

        if not self.engine.dialect.has_schema(connection=self.con, schema_name=schema):
            self.con.execute(CreateSchema(schema))
        else:
            print(f'schema: `{schema}` exists.')
            if if_exists == 'fail':
                print('creation failed')
            elif if_exists == 'replace':
                if not self.drop_schema(schema=schema):
                    return
                self.con.execute(CreateSchema(schema))

            elif if_exists == 'skip':
                return
        print(f'schema: `{schema}` created')

    def drop_schema(self, schema):
        if input(f'drop `{schema}`?("yes" to continue or any other string to abort)') == 'yes':

            if self.engine.dialect.has_schema(connection=self.con, schema_name=schema):
                self.con.execute(DropSchema(schema))

                print(f'schema `{schema}` dropped.')

                return True
            else:
                print(f'schema `{schema}` doesnt exist. dropping failed.')
        else:
            print('aborted')
            return False

    def table_exists(self, table_name):
        return self.engine.dialect.has_table(
            connection=self.con,
            table_name=table_name
        )

    @check_param_valid_range(['if_table_exists'], [['continue', 'fail', 'replace']])
    def create_table(self, table_name, cols, auto_id=None, if_table_exists='continue'):
        self.execute(
            self.stmt_create_table(
                table_name=table_name,
                cols=cols,
                auto_id=auto_id,
                if_table_exists=if_table_exists
            )
        )

    def stmt_show_tables(self):
        return f'SHOW TABLES FROM `{self.schema}`;'

    def stmt_show_cols(self, table_name):
        return f'SHOW COLUMNS FROM `{self.schema}`.`{table_name}`;'
    
    @print_stmt
    @print_action
    def stmt_drop_table(self, table_name):
        print(f'dropping table: `{self.schema}`.`{table_name}`\n')

        stmt = f'DROP TABLE IF EXISTS `{self.schema}`.`{table_name}`'
        return stmt

    @print_stmt
    @print_action
    @check_param_valid_range(['if_table_exists'], [['continue', 'fail', 'replace']])
    def stmt_create_table(
            self,
            table_name,
            cols: List[Column],
            auto_id=None,
            if_table_exists='continue'
    ):

        print(f'creating table: `{self.schema}`.`{table_name}`\n'
              f'if exist: {if_table_exists}')

        if self.table_exists(table_name=table_name):
            print(f'`{table_name}` exists.')
            if if_table_exists == 'continue':
                return ''
            elif if_table_exists == 'fail':
                print('create table failed')
                return ''
            elif if_table_exists == 'replace':
                print('replacing')
                self.execute(
                    self.stmt_drop_table(table_name=table_name)
                )

        if auto_id:
            if auto_id not in [col.name for col in cols]:
                cols = [
                    Column(auto_id, Integer, primary_key=True, autoincrement=True),
                    *cols
                ]

        table = Table(
            table_name,
            self.metadata,
            *cols,
        )
        stmt = CreateTable(table).compile().string.strip()

        return stmt

    @print_stmt
    @print_action
    def stmt_drop_cols(self, table_name, col_names):

        print(f'dropping cols if exist: {col_names}')
        exist_cols = [
            col.Field for col in
            self.execute(
                self.stmt_show_cols(table_name=table_name)
            )
        ]
        col_names = set(col_names) & set(exist_cols)

        if len(col_names) == 0:
            return ''

        col_str = ', \n'.join([f'DROP {col_name}' for col_name in col_names])
        stmt = f'ALTER TABLE `{self.schema}`.`{table_name}` \n' \
               f'{col_str};'

        return stmt

    @print_stmt
    @print_action
    @check_param_valid_range(['if_exists'], [['fail', 'skip', 'replace']])
    def stmt_add_cols(self, table_name, cols: List[Column], if_exists='fail'):

        print(f'adding cols: \n'
              f'{[col.name for col in cols]} \n'
              f'into table: `{self.schema}`.`{table_name}`\n')

        exist_cols = [
            col.Field for col in self.execute(
                self.stmt_show_cols(table_name=table_name)
            )
        ]
        conflict_col_names = []
        for col in cols:
            if col.name in exist_cols:
                if if_exists == 'fail':
                    conflict_col_names.append(col.name)

        if len(conflict_col_names) > 0:
            print(f'conflict cols: {conflict_col_names}')
            if if_exists == 'fail':
                print('add col(s) failed')
                return ''
            elif if_exists == 'skip':
                print(f'skipping cols: {conflict_col_names}')
                cols = [col for col in cols if col.name not in conflict_col_names]
            elif if_exists == 'replace':
                print(f'dropping cols: {conflict_col_names}')
                self.stmt_drop_cols(table_name=table_name, col_names=conflict_col_names)

        col_str = ', \n'.join(
            [
                f'ADD {CreateColumn(col).compile().string.strip()}'
                for col in cols
            ]
        )
        stmt = f'ALTER TABLE `{self.schema}`.`{table_name}` \n' \
               f'{col_str};'

        return stmt


# ************************************************************************************
class Test:
    table_name = 'test_sql'

    def __init__(self, curd: CURD):
        self.curd = curd

    def stmt_show_tables(self):
        stmt = self.curd.stmt_show_tables()
        res = self.curd.execute(stmt)
        print(res)
        for item in res:
            print(item[0])

    def stmt_show_cols(self):
        stmt = self.curd.stmt_show_cols(table_name='tables')
        res = self.curd.execute(stmt)
        print(res)
        for item in res:
            print(item.Field)

    def stmt_create_table(self):
        cols = [
            Column('c_int', Integer),
            Column('c_str', String(128)),
            Column('c_dt', DateTime),
            Column('c_num', DECIMAL(22, 2)),
        ]

        stmt = self.curd.stmt_create_table(
            table_name=self.table_name,
            cols=cols,
            auto_id='id',
            if_exists='replace'
        )

        self.curd.execute(stmt)

    def stmt_drop_cols(self):
        col_names = [
            'c_str', 'c_dt'
        ]
        stmt = self.curd.stmt_drop_cols(table_name=self.table_name, col_names=col_names)
        self.curd.execute(stmt)

    def stmt_add_cols(self):
        cols = [
            Column("name", String(30)),
            Column("fullname", String(128)),
        ]

        stmt = self.curd.stmt_add_cols(table_name=self.table_name, cols=cols)
        self.curd.execute(stmt)


if __name__ == '__main__':
    pass
