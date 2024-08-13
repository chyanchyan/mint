# -*- coding: utf-8 -*-

import json
import os.path
from copy import deepcopy
from typing import Literal
import sys
import os

import pandas as pd
from sqlalchemy.exc import OperationalError
from sqlalchemy import text


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    
from mint.sys_init import *
from mint.db.utils import get_tables
from mint.helper_function.hf_string import udf_format, to_json_obj, to_json_str
from mint.helper_function.hf_file import mkdir
from mint.db.tree import *
from mint.api.api_booking_xl_sheet import render_booking_xl_sheet
from mint.helper_function.hf_data import df_to_ant_table_options


def migration_pandas(con, data_path, schema, if_exists):
    name = os.path.basename(data_path)[:-5]
    try:
        data = pd.read_excel(data_path, index_col=False)

        regular_cols = [col for col in data.columns.tolist() if col[:3] != 'dv_']
        dv_cols = [col for col in data.columns.tolist() if col[:3] == 'dv_']
        data_to_db = data[regular_cols]

        if name == 'project':
            table_name = '项目信息'
            row_index = 'name'
        elif name == 'project_level':
            table_name = '项目分级信息'
            row_index = 'name'
        elif name == 'project_change':
            table_name = '项目分级变动信息'
            row_index = 'project_level_name'
        else:
            table_name = name
            row_index = 'id'
        if len(dv_cols) > 0:
            for i, r in data.iterrows():
                for col in dv_cols:
                    if not pd.isna(r[col]):
                        print(
                            f'表：{table_name} \n'
                            f'行：{r[row_index]} \n'
                            f'列：{col.strip("dv_")} \n'
                            f'自： {data_to_db.loc[i, col.strip("dv_")]} \n'
                            f'更新为：{r[col]} \n')
                        print('*' * 50)
                        data_to_db.loc[i, col.strip('dv_')] = r[col]

        data_to_db.to_sql(
            name=name,
            con=con,
            schema=schema,
            if_exists=if_exists,
            index=False
        )
    except FileNotFoundError:
        print(f'table: {name} xlsx file not exist')
    except OperationalError as e:
        print(traceback.format_exc())
        raise e


def migrate_data_from_xl_folder(
        folder,
        schema,
        booking_sequence,
        if_exists: Literal["fail", "replace", "append"] = "append"
):
    table_names = []
    for file in os.listdir(folder):
        if file.endswith('.xlsx') and file[0] != '~':
            table_names.append(file[:-5])

    table_names.sort(key=lambda x: booking_sequence.index(x))
    
    con = get_con()
    for table_name in table_names:
        file_name = os.path.join(folder, table_name + '.xlsx')
        print(f'migrating {file_name}')
        migration_pandas(
            con=con,
            data_path=file_name,
            schema=schema,
            if_exists=if_exists
        )
    con.close()


def get_right_angle_trees(
        root,
        index_col=None,
        index_values=None,
        limit=0,
        offset=None,
        file_name_str=None,
        stash_uuid=None,
        **kwargs
):
    con = get_con('data')
    tables = get_tables('data')
    tree = Tree(con=con, tables=tables, root=root)
    if file_name_str is not None and file_name_str != '':
        file_names = file_name_str.split(';')
        file_path = os.path.join(PATH_UPLOAD, file_names[0])
        dfs = pd.read_excel(file_path, sheet_name=None)
        t = DataTree(tree=tree)
        t.from_excel_booking_sheet(dfs=dfs)
    else:
        if stash_uuid is not None:
            t = DataTree(tree=tree)
            relevant_data_set_res = con.execute(
                text(
                    'select root, relevant_data_set from stash where stash_uuid = :stash_uuid'
                ),
                {'stash_uuid': stash_uuid}
            )
            if relevant_data_set_res.rowcount > 0:
                relevant_data_set = to_json_obj(relevant_data_set_res.fetchone()[1])
                relevant_data_set = {
                    k: pd.DataFrame(v)
                    for k, v in relevant_data_set.items()
                    if len(v) > 0
                }
                t.from_relevant_data_set(relevant_data_set=relevant_data_set)
        else:
            if int(limit) == 0:
                t = tree
            else:
                t = DataTree(tree=tree)
                t.from_sql(
                    index_col=index_col,
                    index_values=set(index_values),
                    limit=limit,
                    offset=offset
                )

    res = get_right_angle_trees_from_tree_from_tree(tree=t)
    res = [t.json_obj for t in res]
    con.close()
    return res


# @profile_line_by_line
def get_nested_values(
        root,
        limit=None,
        offset=None,
        index_col: str = None,
        index_values=(),
        full_detail=False,
        **kwargs
):
    con = get_con('data')
    tables = get_tables('data')
    dtree = DataTree(root=root, con=con, tables=tables)
    dtree.from_sql(limit=limit, offset=offset, index_col=index_col, index_values=index_values)

    dtree.fill_na_with_none()
    json_obj = dtree.nested_values(full_detail=full_detail)
    json_obj['dataSource'][0].sort(key=lambda x: x['id'], reverse=True)
    con.close()

    json_str = to_json_str(json_obj=json_obj)
    json_obj = to_json_obj(json_str=json_str)
    return json_obj


def get_booking_structure(in_json_obj):
    branch = in_json_obj['branch']
    con = get_con()
    tables = get_tables('data')
    t_branch = Tree(con=con, tables=tables)
    dtree = DataTree(con=con, tables=tables, root=branch)

    select_values = dtree.get_parents_select_values()

    json_obj = {'field_structure': t_branch.json_obj, 'select_values': select_values}

    return json_obj


def mark_ref_value(root, rows, parents, tables):
    parents_output = []
    for p_name, p_data in parents.items():
        for ref_col, ref_data in p_data.items():
            reffed_col = tables[root].cols[ref_col].foreign_key.split('.')[1]
            for ref_cell_id, ref_cell_data in ref_data.items():
                p_data = ref_cell_data[0]
                p_data = mark_auto_name(d=p_data, table=tables[p_name])
                p_d = {p_name: p_data}
                p_d, extra_parents_output = fetch_data_from_dict(d=p_d, tables=tables)
                parents_output.append(p_d)
                parents_output.extend(extra_parents_output)
                try:
                    reffed_value = list(p_d[p_name][reffed_col][0].values())[0]
                except KeyError:
                    raise KeyError
                for i, cell in enumerate(rows[ref_col]):
                    if list(cell.keys())[0] == ref_cell_id:
                        rows[ref_col][i][ref_cell_id] = reffed_value
                        break

    return rows, parents_output


def fetch_data_from_dict(d, tables):
    root, data = list(d.items())[0]
    rows = {}
    parents = {}
    for item_name, item_data in data.items():
        if isinstance(item_data, dict):
            # parent value
            try:
                parents[item_name].update(item_data)
            except KeyError:
                parents[item_name] = item_data
            pass
        else:
            # row value
            try:
                rows[item_name].update(item_data)
            except KeyError:
                rows[item_name] = item_data

    rows, parents_output = mark_ref_value(root=root, rows=rows, parents=parents, tables=tables)

    return {root: rows}, parents_output


def mark_child_name(d_dfs, tree):
    res = dict(d_dfs)

    root_df = d_dfs[tree.root]
    try:
        root_df['name']
    except KeyError:
        naming_from = tree.table.naming_from
        name_ref_col = [
            col for col_name, col in tree.table.cols.items()
            if col.foreign_key
        ]
        name_ref_col = [col for col in name_ref_col if col.foreign_key.split('.')[0] == naming_from][0]
        root_data_tree = DataTree(tree=tree)
        root_data_tree.from_sql(
            index_col=name_ref_col.col_name,
            index_values={root_df[name_ref_col.col_name].values[0]}
        )
        root_df['name'] = root_df.apply(
            lambda x: '-'.join([x[name_ref_col.col_name], tree.root, str(len(root_data_tree.data))]),
            axis=1
        )

    for c in tree.children:
        try:
            c_df = d_dfs[c.root]
        except KeyError:
            continue
        if len(c_df) > 0:
            c_df[c.ref] = d_dfs[c.table.naming_from][c.reffed].values[0]
            c_df['name'] = c_df.apply(
                lambda x: '-'.join([x[c.ref], c.root, str(x.name)]),
                axis=1
            )
            res[c.root] = c_df
            mark_child_name(d_dfs=res, tree=c)
        else:
            c_df.columns = ['name'] + list(c_df.columns)

    return res


def mark_auto_name(d, table):
    naming_cols = [
        (col_name, col.naming_field_order)
        for col_name, col in table.cols.items()
        if not pd.isna(col.naming_field_order)
    ]
    if len(naming_cols) > 0:
        naming_cols = [field for field, order in sorted(naming_cols, key=lambda x: x[1])]
        d['name'] = [{
            'name':
            '-'.join([list(d[field][0].values())[0] for field in naming_cols])
        }]
    return d


def strip_values(d_dfs):
    res = {}
    for key, df in d_dfs.items():
        res[key] = df

    return res


def fill_empty_cells_to_null(df: pd.DataFrame):
    return df.replace(to_replace="", value=None)


def merge_booking_nodes_rows(nodes_data_list):
    res = {}
    for node_data in nodes_data_list:
        node_root = list(node_data.keys())[0]
        node_rows = list(node_data.values())[0]

        try:
            for col in res[node_root].keys():
                res[node_root][col].extend(node_rows[col])
        except KeyError:
            res[node_root] = node_rows

    return res


def dict_to_df(d):
    df = pd.DataFrame(columns=list(d.keys()))
    for col_name, col_data in d.items():
        df[col_name] = [list(cell.values())[0] for cell in col_data]

    return df


def tree_dict_to_json(tree_dict):
    if len(tree_dict) == 0:
        return {}
    res = deepcopy(tree_dict)
    res['table'] = res['table'].to_json_obj()
    res['parents'] = {}
    res['children'] = {}
    for p_name, parent in tree_dict['parents'].items():
        parent = tree_dict_to_json(parent)
        res['parents'][p_name] = parent

    for c_name, child in tree_dict['children'].items():
        child = tree_dict_to_json(child)
        res['children'][c_name] = child
    return res


def gen_booking_xl_sheet_file(root, row_id=''):
    con = get_con('data')
    tables = get_tables('data')
    timestamp = dt.now().strftime("%Y%m%d_%H%M%S_%f")
    dtree = DataTree(root=root, con=con, tables=tables)
    if row_id != "":
        dtree.from_sql(index_col='id', index_values={row_id})
        p_name = dtree.relevant_data_set[root]["name"].values[0]
    else:
        p_name = '录入模板'

    template_path = os.path.join(PATH_ROOT, 'api', 'booking_xl_template.xlsm')
    output_folder = os.path.join(PATH_OUTPUT, 'booking_xl_sheet')
    mkdir(output_folder)
    output_filename = f'booking_excel-{p_name}-{timestamp}.xlsm'
    output_path = os.path.join(
        output_folder,
        output_filename
    )

    render_booking_xl_sheet(
        output_path=output_path,
        data_tree=dtree,
        template_path=template_path
    )
    con.close()
    return {
        'filePath': output_path,
        'fileName': output_filename,
    }


def migrate_from_xlsx(folder, schema_tags=None):
    con = get_con()

    if schema_tags is None:
        schema_tags = get_schema_tags()

    cst_pki = get_cst_pki(con=con, schemas=[get_schema(schema_tag) for schema_tag in schema_tags])
    booking_sequence = get_booking_sequence(cst_pki=cst_pki)

    for schema_tag in schema_tags:
        schema = get_schema(schema_tag)
        schema_folder = os.path.join(folder, schema)
        migrate_data_from_xl_folder(
            folder=schema_folder,
            schema=schema,
            booking_sequence=booking_sequence
        )
    con.close()


def stash(jo):
    root = jo['root']
    stash_uuid = jo['stashUuid']
    relevant_data_set = jo['relevantDataSet']
    relevant_data_set = to_json_str(relevant_data_set)
    comment = jo['comment']

    con = get_con('data')

    is_exist = check_unique({
        'table_name': 'stash',
        'col_name': 'stash_uuid',
        'value': stash_uuid
    })

    if not is_exist:
        sql = ('INSERT INTO stash (stash_uuid, root, relevant_data_set, comment) '
               'VALUES (:stash_uuid, :root, :relevant_data_set, :comment)')
        result = con.execute(
            text(sql),
            {
                'root': root,
                'stash_uuid': stash_uuid,
                'relevant_data_set': relevant_data_set,
                'comment': comment,
            }
        )
        stash_uuid = result.lastrowid
    else:
        sql = ('UPDATE stash SET '
               'root = :root, relevant_data_set = :relevant_data_set, comment = :comment '
               'WHERE stash_uuid = :stash_uuid')
        con.execute(
            text(sql),
            {
                'root': root,
                'stash_uuid': stash_uuid,
                'relevant_data_set': relevant_data_set,
                'comment': comment,
            }
        )
    con.close()
    return {'stashUuid': stash_uuid}


def get_stash_list():
    con = get_con('data')
    table = get_tables('data')['stash']

    col_names = [
        'id',
        'stash_uuid',
        'root',
        'comment'
    ]
    titles = [col.label for col in table.cols if col.col_name in col_names]
    data_types = [col.data_type for col in table.cols if col.col_name in col_names]

    sql = 'SELECT * FROM stash'
    df = pd.read_sql(sql, con=con).sort_values('id', ascending=False)[col_names]
    con.close()

    res = df_to_ant_table_options(df=df, titles=titles, data_types=data_types)
    return res


def get_select_options(jo):
    table_name = jo['table_name']
    col_name = jo['col_name']
    sql = 'SELECT DISTINCT {} FROM {}'.format(col_name, table_name)
    con = get_con('data')
    df = pd.read_sql(sql, con=con)
    con.close()
    res = df[col_name].tolist()
    return res


def check_unique(jo):
    table_name = jo['table_name']
    col_name = jo['col_name']
    value = jo['value']
    sql = 'SELECT * FROM {} WHERE {} = :value'.format(table_name, col_name)
    con = get_con('data')
    result = con.execute(text(sql), {'value': value})
    con.close()
    return result.rowcount > 0
