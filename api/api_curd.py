import sys
import os

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.helper_function.hf_db import dfs_to_db
from mint.sys_init import *
from mint.db.tree import Tree, DataTree, get_flattened_tree_list_from_right_angle_trees
from mint.db.utils import get_tables
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from typing import Dict


def get_project_level_change_init_rows(df_p: pd.DataFrame, df_l: pd.DataFrame):
    plnc_cols = [
        'project_level_name',
        'change_date',
        'notional_delta',
        'notional_to',
        'comment'
    ]

    plrc_cols = [
        'project_level_name',
        'change_date',
        'rate_delta',
        'rate_to',
        'comment'
    ]

    df_pl = pd.merge(
        df_l.add_prefix('l.'),
        df_p.add_prefix('p.'),
        left_on='l.project_name',
        right_on='p.name',
        how='left'
    )
    df_pl['project_level_name'] = df_pl['l.name']
    df_pl['change_date'] = df_pl['p.st_date']
    df_pl['comment'] = 'init'
    df_pl['rate_delta'] = df_pl['l.rate']
    df_pl['rate_to'] = df_pl['l.rate']
    df_pl['notional_delta'] = df_pl['p.notional'] * df_pl['l.weights'] / df_pl['l.weights'].sum()
    df_pl['notional_to'] = df_pl['notional_delta']

    return df_pl[plnc_cols], df_pl[plrc_cols]


def get_dtree_from_xl_file(con, root, file_path, tables):
    dtree = DataTree(root=root, con=con, tables=tables)
    dfs = pd.read_excel(
        file_path,
        sheet_name=None
    )
    dtree.from_excel_booking_sheet(dfs=dfs)
    return dtree


def booking_from_xl_file(con, root, file_path, tables):
    dtree = get_dtree_from_xl_file(con=con, root=root, file_path=file_path, tables=tables)

    if root == 'project':
        dfs = dtree.relevant_data_set
        # add project_change
        df_p = dfs['project']
        df_pl = dfs['project_level']

        df_plnc, df_plrc = get_project_level_change_init_rows(df_p=df_p, df_l=df_pl)

        dtree.relevant_data_set['project_level_notional_change'] = df_plnc
        dtree.relevant_data_set['project_level_rate_change'] = df_plrc

    for k, v in dtree.relevant_data_set.items():
        print(k)
        print(v)
        print('*' * 100)

    dfs_to_db(
        con=con,
        d_dfs=dtree.relevant_data_set,
        tree=dtree,
        schema=get_schema(schema_tag='data')
    )


def booking_from_relevant_data_set_json(con, root, data_json, **kwargs):

    relevant_data_set = {
        table_root: pd.DataFrame(
            data=values,
            columns=[col.col_name for col in TABLES[table_root].cols]
        )
        for table_root, values in data_json.items()
    }

    if root == 'project':
        df_p = relevant_data_set['project']
        df_l = relevant_data_set['project_level']
        # add init project change
        df_plnc, df_plrc = get_project_level_change_init_rows(
            df_p=df_p,
            df_l=df_l
        )
        relevant_data_set['project_level_notional_change'] = df_plnc
        relevant_data_set['project_level_rate_change'] = df_plrc

    create_tree(con, root, relevant_data_set)


def create(
        con,
        root,
        df: pd.DataFrame,
):
    df.to_sql(root, con=con, if_exists='append', index=False)


def create_tree(
        con,
        root,
        submit_values: Dict[str, pd.DataFrame],
        **kwargs
):
    submit_values = {
        table_root: pd.DataFrame(data=values, columns=[col.col_name for col in TABLES[table_root].cols])
        for table_root, values in submit_values.items()
    }
    dtree = DataTree(root=root, con=con, tables=TABLES)
    dtree.from_relevant_data_set(submit_values)
    trimmed_relevant_data_set = dtree.relevant_data_set

    for root in dtree.booking_sequence:
        df = trimmed_relevant_data_set[root]
        print('-' * 50)
        print(root)
        print('-' * 50)
        print(df)
        print('*' * 100)

        create(con, root, df)


def delete(
        con,
        root,
        df: pd.DataFrame,
        index_col=None,
        index_values=None,
):
    print(f'[sql] deleting from {root} where `{index_col}` in ({", ".join(map(str, index_values))})')
    if index_col is None:        
        print('No index column specified. nothing happens.')
        return
    if index_values is None:
        print('No index values specified. nothing happens.')
        return
    
    index = df[index_col].tolist()
    sql = f'delete from {root} where `{index_col}` in ({", ".join(map(str, index))})'
    con.execute(text(sql))
    

def delete_tree(
        con,
        root,
        index_col=None,
        index_value=None,
        preview=True,
        **kwargs
):
    if index_value is None:
        return
    
    dtree = DataTree(root=root, con=con, tables=TABLES)
    dtree.from_sql(index_col=index_col, index_values={index_value})
    
    relevant_data_set = dtree.relevant_data_set

    preview_data = []
    bs = dtree.all_childhood_names()
    bs = [root] + list(bs)
    for table_root in bs:
        try:
            df = relevant_data_set[table_root]
        except KeyError:
            continue
        col_objs = TABLES[table_root].cols
        columns = [col.col_name for col in col_objs if col.web_visible == 1]
        df = df[columns]
        df = df.rename(columns={col.col_name: col.label for col in col_objs})
        preview_data.append({
            'label': TABLES[table_root].label,
            'data': df.to_dict(orient='split')
        })
    if preview:
        return preview_data
    else:
        df = relevant_data_set[root].reset_index()
        delete(con, root, df, index_col=index_col, index_values=[index_value])


def update_tree(
        root,
        relevant_data_set: Dict[str, pd.DataFrame],
        algo_func=None,
        index_col=None,
        index_value=None,
        **kwargs
):
    if algo_func is not None:
        relevant_data_set = algo_func(relevant_data_set, **kwargs)

    con = get_con('data')

    dtree = DataTree(root=root, con=con, tables=TABLES)
    dtree.from_relevant_data_set(relevant_data_set)
    trimmed_relevant_data_set = dtree.relevant_data_set
    all_childhood_names = dtree.all_childhood_names()
    for table_root in dtree.booking_sequence:
        df = trimmed_relevant_data_set[table_root]
        cols = df.columns.tolist()
        
        if table_root == root:
            delete_tree(con, root, index_col=index_col, index_value=index_value)
            create(con, root, df)

        elif table_root is dtree.all_parenthood_names():
            # 构造 INSERT 语句
            insert_stmt = f"INSERT INTO {table_root} ({', '.join([f'`{col}`' for col in cols])}) VALUES "

            # 构造 VALUES 占位符
            values_stmt = f"({', '.join([':' + col for col in cols])})"

            # 构造 ON DUPLICATE KEY UPDATE 部分
            update_stmt = ', '.join([f"`{col}` = VALUES({col})" for col in cols if col != 'id'])

            # 拼接完整 SQL 语句
            sql = f"{insert_stmt}{values_stmt} ON DUPLICATE KEY UPDATE {update_stmt}"

            # 将 DataFrame 转换为字典列表
            data = df.replace(np.nan, None).to_dict(orient='records')

            # 使用连接执行 SQL 语句
            for record in data:
                con.execute(text(sql), record)
                
        elif table_root in all_childhood_names:
            create(con, root, df)


