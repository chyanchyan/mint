import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.helper_function.hf_db import dfs_to_db
from mint.sys_init import *
from mint.db.tree import Tree, DataTree, get_flattened_dtrees_from_dtree_list
from mint.db.utils import get_tables


def booking(in_json_obj):
    # user_name = in_json_obj['user_name']
    root = in_json_obj['data']['root']
    data = in_json_obj['data']['data']
    schema = get_schema('data')
    tables = get_tables('data')
    
    nodes = []
    for d in data:
        node, parents = fetch_data_from_dict(d=d, tables=tables)
        nodes.append(node)
        nodes.extend(parents)

    d_dfs_ = merge_booking_nodes_rows(nodes)
    d_dfs = {}
    for node_root, d_rows in d_dfs_.items():
        if len(d_rows) > 0:
            d_dfs[node_root] = dict_to_df(d=d_rows)

    for node_root, df in d_dfs.items():
        df = df.replace(to_replace="", value=None)
        print(df)
        table = tables[node_root]
        for col in df.columns:
            if table.cols[col].unique or table.cols[col].foreign_key:
                df = df.drop_duplicates(subset=df.columns)
        d_dfs[node_root] = df
    print(d_dfs)
    con = get_con()
    tree = Tree(con=con, tables=tables, root=root)

    d_dfs = mark_child_name(d_dfs=d_dfs, tree=tree)
    d_dfs = strip_values(d_dfs=d_dfs)

    dfs_to_db(d_dfs=d_dfs, tree=tree, schema=schema)


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
    gs = df_pl.groupby('l.name')
    df_pl['notional_delta'] = gs['p.notional'] * gs['l.weights'] / gs['l.weights'].sum()
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


def booking_from_datatree_list_json(con, root, dtree_list):
    tables = get_tables('data')
    relevant_data_set = {}

    dtrees = get_flattened_dtrees_from_dtree_list(dtree_list=dtree_list)

    for dtree in dtrees:
        relevant_data_set[dtree['root']] = pd.DataFrame(dtree['values'])

    if root == 'project':
        df_p = relevant_data_set['project']
        df_l = relevant_data_set['project_level']
        # add init project change
        df_plnc, df_plrc = get_project_level_change_init_rows(df_p=df_p, df_l=df_l)
        relevant_data_set['project_level_notional_change'] = df_plnc
        relevant_data_set['project_level_rate_change'] = df_plrc

    dtree = DataTree(root=root, con=con, tables=tables)
    dtree.from_relevant_data_set(relevant_data_set)
    for root in dtree.booking_sequence:
        df = dtree.relevant_data_set[root]
        print(root)
        print(df)
        print('*' * 100)
        df.to_sql(root, con=con, if_exists='append', index=False)

