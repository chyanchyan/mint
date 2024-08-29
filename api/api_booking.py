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


def booking_from_relevant_data_set_json(jo):

    con = get_con('data')
    tables = get_tables('data')
    root = jo['root']
    relevant_data_set_json = jo['relevantDataSet']
    relevant_data_set = {
        table_root: pd.DataFrame(
            data=values,
            columns=[col.col_name for col in tables[table_root].cols]
        )
        for table_root, values in relevant_data_set_json.items()
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

    dtree = DataTree(root=root, con=con, tables=tables)
    dtree.from_relevant_data_set(relevant_data_set)
    for root in dtree.booking_sequence:
        df = dtree.relevant_data_set[root]
        print('-' * 50)
        print(root)
        print('-' * 50)
        print(df)
        print('*' * 100)
        try:
            df.to_sql(root, con=con, if_exists='append', index=False)
        except IntegrityError as e:
            if e.orig.args[0] == 1452:
                print('IntegrityError: ', e.args[0])
                print('This is probably because of a foreign key constraint.')
            elif e.orig.args[0] == 1062:
                # update value base on duplicate entry
                dup_key = e.orig.args[1].split("for key '")[1].split("'")[0]
                if root == 'project':
                    print('Duplicate entry in project. updating')
                    for i, row in df.iterrows():
                        name = row['name']
                        sql = f'delete from project where name = "{name}"'
                        con.execute(text(sql))
                    df.to_sql(root, con=con, if_exists='append', index=False)
                else:
                    print('Duplicate entry in parents. updating')
                    for i, row in df.iterrows():

                        sql = (
                            f'update {root} set '
                            f'{dup_key} = {row[dup_key]} '
                            f'where `{dup_key}` = {row[dup_key]}'
                        )


if __name__ == '__main__':
    con = get_con('data')
    df = pd.DataFrame({
        'name': ['中诚信托安鑫2号（第7期）集合资金信托'],
        'notional': 1000,
        'annual_days': 365,
        'st_date': dt(2024, 1, 1),
        'type_ledger_detail': 'abs集合信托',
        'type_predict': '非标-非放款池',
        'project_credit_name': '未指定',
        'type_scene': '其他',
        'is_fed_project': 1,

    })
    df.to_sql('project', con=con, if_exists='append', index=False)
    print(df)