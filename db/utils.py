"""
数据库工具模块

本模块提供了数据库连接、查询、数据填充等常用功能的工具函数。
主要用于处理数据库连接、数据验证、缺失值填充等操作。
"""

import traceback
from copy import copy
from datetime import datetime as dt
import pandas as pd
import numpy as np
import pymysql
import sqlalchemy.exc
from dns.dnssec import key_id
from sqlalchemy import create_engine

import os
import sys

from mint.helper_function.hf_string import get_first_letter_of_dash_name

# 添加父目录到系统路径，以便导入其他模块
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.globals import *
from mint.meta.table_objs import get_tables_from_info


def db_get_schema(schema_tag, sys_mode, project_name):
    """
    根据系统模式和项目名称生成数据库schema名称
    
    Args:
        schema_tag (str): schema标签
        sys_mode (str): 系统模式（PROD表示生产环境）
        project_name (str): 项目名称
    
    Returns:
        str: 完整的schema名称
    """
    if sys_mode == 'PROD':
        return f'{project_name}_{schema_tag}'
    else:
        return f'{project_name}_{schema_tag}_{sys_mode}'


def db_get_url(**db_params):
    """
    根据数据库参数生成数据库连接URL
    
    Args:
        **db_params: 数据库连接参数字典，包含：
            - db_type: 数据库类型（默认mysql+pymysql）
            - db_username: 用户名
            - db_password: 密码
            - db_host: 主机地址
            - db_port: 端口号
            - schema: 数据库名称
            - auto_commit: 是否自动提交
            - db_charset: 字符集（默认utf8）
    
    Returns:
        str: 数据库连接URL
    """
    try:
        db_type = db_params['db_type']
    except KeyError:
        db_type = 'mysql+pymysql'

    username = db_params['db_username']
    password = db_params['db_password']
    host = db_params['db_host']
    port = db_params['db_port']
    schema = db_params['schema']
    auto_commit = db_params['auto_commit']

    try:
        charset = db_params['db_charset']
    except KeyError:
        charset = 'utf8'

    return str(
        f'{db_type}://{username}:{password}@{host}:{port}/'
        f'{schema}?charset={charset}{auto_commit and "&autocommit=true" or ""}'
    )


def db_get_engine(**db_params):
    """
    创建SQLAlchemy数据库引擎
    
    Args:
        **db_params: 数据库连接参数
    
    Returns:
        sqlalchemy.engine.Engine: 数据库引擎对象
    """
    url = db_get_url(**db_params)
    return create_engine(url=url, pool_recycle=1800, pool_pre_ping=True)


def db_connect_db(create_if_not_exist=True, auto_commit=True, **db_params):
    """
    连接数据库，如果数据库不存在则创建
    
    Args:
        create_if_not_exist (bool): 如果数据库不存在是否创建
        auto_commit (bool): 是否自动提交
        **db_params: 数据库连接参数
    
    Returns:
        tuple: (engine, connection, url) 数据库引擎、连接和URL
    
    Raises:
        sqlalchemy.exc.OperationalError: 数据库连接错误
    """
    username = db_params['db_username']
    password = db_params['db_password']
    host = db_params['db_host']
    port = db_params['db_port']
    schema = db_params['schema']

    try:
        charset = db_params['db_charset']
    except KeyError:
        charset = 'utf8'

    url = db_get_url(**db_params, auto_commit=auto_commit)
    engine = db_get_engine(**db_params, auto_commit=auto_commit)

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


def db_get_schema_tags(con):
    """
    获取所有schema标签列表
    
    Args:
        con: 数据库连接对象
    
    Returns:
        list: schema标签列表
    """
    return pd.read_sql(
        sql='select `schema_tag` from `schemas`',
        con=con
    )['schema_tag'].tolist()


def db_get_first_delta_to_null_rows_and_all_rest_rows(
        con,
        table_name: str,
        index: str,
        date_col: str,
        delta_col: str = None,
        to_col: str = None
):
    """
    获取每个索引值从第一个空值行开始的所有后续行
    
    这个函数用于查找数据中缺失值的起始位置，并返回从该位置开始的所有数据。
    
    Args:
        con: 数据库连接对象
        table_name (str): 表名
        index (str): 索引列名
        date_col (str): 日期列名
        delta_col (str, optional): 增量列名
        to_col (str, optional): 目标值列名
    
    Returns:
        pd.DataFrame: 包含从第一个空值开始的所有行的数据框
    """
    sql = f"""
            WITH ranked_data AS (
                SELECT 
                    `id`,
                    `{index}`,
                    `{date_col}`,
                    {f"`{delta_col}`, " if delta_col is not None else ''}
                    {f"`{to_col}`, " if to_col is not None else ''}
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
                    {' OR '.join([f"`{col}` IS NULL" for col in [delta_col, to_col] if col is not None])}
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


def db_get_last_non_null_rows(
        con,
        table_name: str,
        index: str,
        date_col: str,
        target_col: str
):
    """
    获取每个索引值的目标列最后一个非空值对应的完整行
    
    Args:
        con: 数据库连接对象
        table_name (str): 表名
        index (str): 索引列名
        date_col (str): 日期列名
        target_col (str): 目标列名
    
    Returns:
        pd.DataFrame: 包含每个索引最后一个非空值行的数据框
    """
    sql = f"""
        SELECT t1.*
        FROM {table_name} t1
        JOIN (
            SELECT `{index}`, MAX(`{date_col}`) AS max_date
            FROM {table_name}
            WHERE `{target_col}` IS NOT NULL
            GROUP BY `{index}`
        ) t2
        ON t1.`{index}` = t2.`{index}` AND t1.`{date_col}` = t2.max_date
        WHERE t1.`{target_col}` IS NOT NULL;
    """
    res = pd.read_sql(
        sql=sql,
        con=con
    )
    return res


def db_fill_change_null_value(
        df_to_fill: pd.DataFrame,
        table_name,
        index,
        date_col,
        delta_col,
        to_col,
        validate_by_to: bool = True,
        is_sorted = False
):
    """
    填充数据框中的空值，确保delta列和to列的数据一致性
    
    这个函数通过以下逻辑填充缺失值：
    1. 如果第一天两个值都缺失，根据数据量决定填充策略
    2. 如果期间某个值缺失，根据validate_by_to参数决定是否以to值为准
    3. 如果两个值都存在但不一致，根据validate_by_to参数决定保留哪个值
    
    Args:
        df_to_fill (pd.DataFrame): 需要填充的数据框
        table_name (str): 表名
        index (str): 索引列名
        date_col (str): 日期列名
        delta_col (str): 增量列名
        to_col (str): 目标值列名
        validate_by_to (bool): 是否以to值为准进行验证和填充
        is_sorted (bool): 数据是否已按日期排序
    
    Returns:
        tuple: (填充后的数据框, 更新SQL语句列表)
    """
    digits = 8  # 精度位数
    if not is_sorted:
        df_to_fill_copy = df_to_fill.sort_values(date_col)
    else:
        df_to_fill_copy = df_to_fill.copy()
    df_to_fill_copy = df_to_fill_copy.replace(np.nan, None).replace(pd.NaT, None)
    if len(df_to_fill_copy) == 0:
        return df_to_fill_copy, []

    df_filled = df_to_fill_copy.copy()
    fill_sqls = []
    for index_value, g in df_to_fill_copy.groupby(index):
        last_to = 0
        for i, (row_index, g_row) in enumerate(g.iterrows()):
            try:
                row_id = g_row['id']
            except KeyError:
                prefix = get_first_letter_of_dash_name(table_name)
                row_id = g_row[f'{prefix}.id']
            delta_value = copy(g_row[delta_col])
            to_value = copy(g_row[to_col])

            u_to_value = copy(to_value)
            u_delta_value = copy(delta_value)
            if i == 0:
                if delta_value is None and to_value is None:
                    if len(g) == 1:
                        # 若只有一行数据，则将缺失值设为0
                        u_delta_value = 0
                        u_to_value = 0
                    else:
                        # 若第一天就缺失全部值，则 `to` 等于第二天的值
                        u_delta_value = 0
                        u_to_value = copy(g.iloc[1, :][to_col])

                # 若第一天就缺失某一个值，则等于另一个值
                elif delta_value is None:
                    u_delta_value = round(to_value, digits)
                    u_to_value = round(to_value, digits)
                elif to_value is None:
                    u_delta_value = round(delta_value, digits)
                    u_to_value = round(delta_value, digits)
            else:
                # 若期间缺失某一个值
                if delta_value is None and to_value is not None:
                    if validate_by_to:
                        u_delta_value = round(to_value - last_to, digits)
                        u_to_value = round(to_value, digits)
                    else:
                        u_delta_value = 0
                        u_to_value = copy(last_to)
                elif delta_value is not None and to_value is None:
                    if validate_by_to:
                        u_delta_value = round(-last_to, digits)
                        u_to_value = 0
                    else:
                        u_to_value = round(last_to + delta_value, digits)

                # 如果都缺失，则默认按照to
                elif delta_value is None and to_value is None:
                    u_delta_value = 0
                    u_to_value = round(last_to, digits)

                # 如果都有值，则校验
                elif delta_value is not None and to_value is not None:
                    if round(delta_value, 4) != round(to_value - last_to, digits):
                        if validate_by_to:
                            u_delta_value = round(to_value - last_to, digits)
                            u_to_value = round(to_value, digits)
                        else:
                            u_delta_value = round(delta_value, digits)
                            u_to_value = round(last_to + delta_value, digits)
                    else:
                        u_delta_value = round(delta_value, digits)
                        u_to_value = round(to_value, digits)

            if u_to_value != to_value or u_delta_value != delta_value:
                if row_id is not None:
                    sql = (
                        f'update `{table_name}` set `{delta_col}` = {u_delta_value}, '
                        f'`{to_col}` = {u_to_value} '
                        f'where `id` = {row_id};'
                    )
                else:
                    sql = (
                        f'update `{table_name}` set `{delta_col}` = {u_delta_value}, '
                        f'`{to_col}` = {u_to_value} '
                        f'where `{index}` = \'{index_value}\' and `{date_col}` = \'{g_row[date_col]}\';'
                    )
                df_filled.loc[g_row.name, delta_col] = copy(u_delta_value)
                df_filled.loc[g_row.name, to_col] = copy(u_to_value)

                fill_sqls.append(sql)

            last_to = copy(u_to_value)
    return df_filled, fill_sqls


def db_refresh_delta_by_to(
        df_to_fill: pd.DataFrame,
        index,
        date_col,
        delta_col,
        to_col,
        is_sorted=False
):
    """
    根据to列的值重新计算delta列的值
    
    通过计算to列的差分来更新delta列，确保数据的一致性。
    
    Args:
        df_to_fill (pd.DataFrame): 需要处理的数据框
        index (str): 索引列名
        date_col (str): 日期列名
        delta_col (str): 增量列名
        to_col (str): 目标值列名
        is_sorted (bool): 数据是否已按索引和日期排序
    
    Returns:
        pd.DataFrame: 更新后的数据框
    """
    def get_delta_col(g):
        g[delta_col] = g[to_col].diff().fillna(g[to_col].iloc[0])
        return g

    if not is_sorted:
        df_to_fill_copy = df_to_fill.sort_values([index, date_col])
    else:
        df_to_fill_copy = df_to_fill.copy()
    res = df_to_fill_copy.replace(np.nan, None).replace(pd.NaT, None)
    res = res.groupby(index).apply(get_delta_col).reset_index(drop=True)
    return res


def db_check_change_delta_and_to_value(
        con,
        table_name: str,
        index: str,
        date_col: str,
        delta_col: str,
        to_col: str
):
    """
    检查delta列和to列的值是否一致
    
    验证每个索引值的时间序列中，delta值是否等于to值的差分。
    
    Args:
        con: 数据库连接对象
        table_name (str): 表名
        index (str): 索引列名
        date_col (str): 日期列名
        delta_col (str): 增量列名
        to_col (str): 目标值列名
    
    Returns:
        pd.DataFrame: 包含不一致行的数据框，如果全部一致则返回空数据框
    """
    df = pd.read_sql(
        sql=f'select `{index}`, `{date_col}`, `{delta_col}`, `{to_col}` '
            f'from `{table_name}`',
        con=con
    )
    df = df.sort_values([index, date_col])
    res = []
    for index_name, g in df.groupby(index):
        last_to = None
        for row_num, (i, row) in enumerate(g.iterrows()):
            delta_value = row[delta_col]
            to_value = row[to_col]
            if row_num == 0:
                if round(delta_value, 4) != round(to_value, 4):
                    wrong_row = row.to_frame().T
                    wrong_row['last_to'] = 'first'
                    wrong_row['diff'] = round(delta_value, 4) - round(to_value, 4)
                    res.append(wrong_row)
            else:
                if round(delta_value, 4) != round(to_value - last_to, 4):
                    wrong_row = row.to_frame().T
                    wrong_row['last_to'] = last_to
                    wrong_row['diff'] = round(delta_value, 4) - round(to_value - last_to, 4)
                    res.append(wrong_row)
            last_to = copy(to_value)

    if len(res) > 0:
        return pd.concat(res, ignore_index=True)
    else:
        return pd.DataFrame()


def db_get_df_changes(
        con,
        table_name: str,
        index: str,
        date_col: str,
        delta_col: str = None,
        to_col: str = None,
        st_date: dt = None,
        exp_date: dt = None,
        filter_sql: str = None,
        equal_delta_to: bool = True
):
    """
    获取指定时间范围内的数据变化
    
    获取从开始日期到结束日期之间的所有数据，包括开始日期前的最后一条记录。
    
    Args:
        con: 数据库连接对象
        table_name (str): 表名
        index (str): 索引列名
        date_col (str): 日期列名
        delta_col (str, optional): 增量列名
        to_col (str, optional): 目标值列名
        st_date (dt, optional): 开始日期，默认为1970-01-01
        exp_date (dt, optional): 结束日期，默认为当前时间
        filter_sql (str, optional): 额外的过滤条件
        equal_delta_to (bool): 是否将delta列设置为等于to列（用于开始日期前的记录）
    
    Returns:
        pd.DataFrame: 包含指定时间范围内数据的数据框
    """
    if st_date is None:
        st_date = dt.strptime('1970-01-01', '%Y-%m-%d')
    if exp_date is None:
        exp_date = dt.now()

    sql_less_max = f"""
        SELECT t1.*
        FROM `{table_name}` t1
        JOIN (
            SELECT `{index}`, MAX(`{date_col}`) AS max_date
            FROM `{table_name}`
            WHERE `{date_col}` < "{st_date.strftime('%F %T')}"
            GROUP BY `{index}`    
        ) t2
        ON t1.`{index}` = t2.`{index}` AND t1.`{date_col}` = t2.max_date
        %s
    """

    sql_rest = f"""
        SELECT *
        FROM `{table_name}`
        WHERE (`{date_col}` >= "{st_date.strftime('%F %T')}" 
        AND `{date_col}` < "{exp_date.strftime('%F %T')}") 
        %s 
    """

    if filter_sql:
        sql_less_max = sql_less_max % f'WHERE t1.{filter_sql}'
        sql_rest = sql_rest % f'AND {filter_sql}'
    else:
        sql_less_max = sql_less_max % ''
        sql_rest = sql_rest % ''
    df_less_max = pd.read_sql(
        sql=sql_less_max,
        con=con
    )

    df_less_max['_is_less_max'] = 1
    df_rest = pd.read_sql(
        sql=sql_rest,
        con=con
    )
    if equal_delta_to and delta_col is not None and to_col is not None:
        df_less_max[delta_col] = df_less_max[to_col]
    res = pd.concat([df_less_max, df_rest]).sort_values([index, date_col]).reset_index(drop=True)

    return res


def get_con(schema_tag):
    """
    获取指定schema的数据库连接
    
    Args:
        schema_tag (str): schema标签
    
    Returns:
        sqlalchemy.engine.Connection: 数据库连接对象
    """
    engine, con, url = db_connect_db(
        **DB_PARAMS,
        schema=db_get_schema(
            schema_tag=schema_tag,
            sys_mode=SYS_MODE,
            project_name=PROJECT_NAME
        )
    )
    return con


def get_tables(schema_tag: str = None):
    """
    获取指定schema的所有表信息
    
    Args:
        schema_tag (str, optional): schema标签，如果为None则获取所有schema的表
    
    Returns:
        dict: 表信息字典，键为表名，值为表对象
    """
    if schema_tag is not None:
        filter_sql = f' where `schema_tag` = "{schema_tag}" or `schema_tag` is Null'
    else:
        filter_sql = ''
    con = get_con('core')
    tables_info = pd.read_sql(
        sql='select * from tables' + filter_sql,
        con=con
    ).replace(np.nan, None)
    table_names = tables_info['table_name'].tolist()
    table_names_sql_filter = ', '.join([f'"{table_name}"' for table_name in table_names])
    cols_info = pd.read_sql(
        sql=f'select * from cols where `table_name` in ({table_names_sql_filter})',
        con=con
    ).replace(np.nan, None)
    res = get_tables_from_info(
        tables_info=tables_info,
        cols_info=cols_info,
        get_schema=lambda x: db_get_schema(x, SYS_MODE, PROJECT_NAME)
    )
    con.close()
    return res

