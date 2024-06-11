import traceback
from copy import copy

import pandas as pd
import pymysql
import sqlalchemy.exc
from sqlalchemy import create_engine


def db_get_schema(schema_tag, sys_mode, project_name):
    if sys_mode == 'PROD':
        return f'{project_name}_{schema_tag}'
    else:
        return f'{project_name}_{schema_tag}_{sys_mode}'


def db_connect_db(create_if_not_exist=True, **db_params):

    try:
        db_type = db_params['db_type']
    except KeyError:
        db_type = 'mysql+pymysql'

    username = db_params['db_username']
    password = db_params['db_password']
    host = db_params['db_host']
    port = db_params['db_port']
    schema = db_params['schema']

    try:
        charset = db_params['db_charset']
    except KeyError:
        charset = 'utf8'

    url = str(
        f'{db_type}://{username}:{password}@{host}:{port}/'
        f'{schema}?charset={charset}&autocommit=true'
    )

    engine = create_engine(url=url)
    try:
        con = engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        if e.orig.args[0] == 1049:
            print(f'schema "{schema}" doesnt exist.')
            if create_if_not_exist:
                print('creating...')
                pymysql.connect(
                    host=host,
                    port=int(port),
                    user=username,
                    password=password,
                    charset=charset
                ).cursor().execute(f'create schema {schema}')
                print(f'schema "{schema}" created')
                engine = create_engine(url=url)
                con = engine.connect()
            else:
                traceback.format_exc()
                print(e)
                raise e
        else:
            print(url)
            print('bugs in db url')
            traceback.format_exc()
            print(e)
            raise sqlalchemy.exc.OperationalError

    return engine, con, url


def db_get_con(**db_params):
    engine, con, url = db_connect_db(
        **db_params
    )

    return con


def db_get_delta_to_null_rows(
        con,
        table_name: str,
        index: str,
        date_col: str,
        delta_col: str,
        to_col: str
):
    sql = f"""
            WITH ranked_data AS (
                SELECT 
                    `id`,
                    `{index}`,
                    `{date_col}`,
                    `{delta_col}`,
                    `{to_col}`,
                    ROW_NUMBER() OVER (PARTITION BY `{index}` ORDER BY `{date_col}`) AS rn
                FROM 
                    {table_name}
            ),
            first_null_row AS (
                SELECT
                    `{index}`,
                    MIN(rn) AS first_null_rn
                FROM 
                    ranked_data
                WHERE 
                    `{delta_col}` IS NULL OR `{to_col}` IS NULL
                GROUP BY 
                    `{index}`
            )
            SELECT 
                rd.*
            FROM 
                ranked_data rd
            JOIN 
                first_null_row fnr
            ON 
                rd.`{index}` = fnr.`{index}` AND rd.rn >= COALESCE(fnr.first_null_rn - 1, 1)
            ORDER BY 
                rd.`{index}`, rd.`{date_col}`;
        """

    res = pd.read_sql(
        sql=sql,
        con=con
    )

    return res


def db_fill_change_null_value(
        con,
        table_name: str,
        index: str,
        date_col: str,
        delta_col: str,
        to_col: str
):
    df_to_fill = db_get_delta_to_null_rows(
        con=con,
        table_name=table_name,
        index=index,
        date_col=date_col,
        delta_col=delta_col,
        to_col=to_col
    )
    if len(df_to_fill) == 0:
        print('all data filled')
        return []
    fill_sqls = []
    for index_value, g in df_to_fill.groupby(index):
        last_to = None
        for i, (row_index, g_row) in enumerate(g.iterrows()):
            row_id = g_row['id']
            delta_value = g_row[delta_col]
            to_value = g_row[to_col]
            if pd.isna(delta_value) and pd.isna(to_value):
                # 增量和当量全都为空时报错
                print(g_row)
                print('both to and delta value are null')
                raise ValueError

            sql = None
            if i == 0:
                # 若第一天就缺失某一个值，则等于另一个值
                if pd.isna(delta_value):
                    delta_value = copy(to_value)
                    sql = (
                        f'update `{table_name}` set `{delta_col}` = {delta_value} '
                        f'where `id` = {row_id};'
                    )
                elif pd.isna(to_value):
                    to_value = copy(delta_value)
                    sql = (
                        f'update `{table_name}` set `{to_col}` = {delta_value} '
                        f'where `id` = {row_id};'
                    )
            else:
                # 若期间缺失某一个值
                if pd.isna(delta_value):
                    delta_value = to_value - last_to
                    sql = (
                        f'update `{table_name}` set `{delta_col}` = {delta_value} '
                        f'where `id` = {row_id};'
                    )
                elif pd.isna(to_value):
                    to_value = last_to + delta_value
                    sql = (
                        f'update `{table_name}` set `{to_col}` = {to_value} '
                        f'where `id` = {row_id};'
                    )

            if sql:
                fill_sqls.append(sql)
            last_to = copy(to_value)

    return fill_sqls
