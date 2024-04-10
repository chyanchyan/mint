import os.path
from copy import deepcopy
from datetime import datetime as dt
from typing import Literal

if 'mint' in __name__.split('.'):
    from .sys_init import *
    from .meta_files.table_objs import get_tables
    from .tree import DataTree, Tree, get_cst_pki, get_booking_sequence
    from .booking_xl_sheet import render_booking_xl_sheet
    from .helper_function.hf_string import udf_format, to_json_obj, to_json_str
    from .helper_function.hf_file import mkdir, get_last_snapshot_timestamp
    from .helper_function.hf_xl import migration_pandas
    from .helper_function.hf_db import df_to_db
    from .helper_function.hf_func import profile_line_by_line
else:
    from sys_init import *
    from meta_files.table_objs import get_tables
    from helper_function.hf_string import udf_format, to_json_obj, to_json_str
    from helper_function.hf_file import mkdir, get_last_snapshot_timestamp
    from helper_function.hf_xl import migration_pandas
    from helper_function.hf_db import df_to_db
    from helper_function.hf_func import profile_line_by_line
    from tree import DataTree, Tree, get_cst_pki, get_booking_sequence
    from booking_xl_sheet import render_booking_xl_sheet

TABLES = get_tables(tables_info=DB_TABLES_INFO, cols_info=DB_COLS_INFO)


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

    for table_name in table_names:
        file_name = os.path.join(folder, table_name + '.xlsx')
        print(f'migrating {file_name}')
        migration_pandas(
            engine=DB_ENGINE,
            data_path=file_name,
            schema=schema,
            if_exists=if_exists
        )


def get_booking_table_names():
    res = [
        {
            'name': table.table_name,
            'web_label': table.label,
            'order': table.web_list_index
        }
        for i, table in TABLES.items()
        if not pd.isna(table.web_list_index)
    ]
    return res


def get_data_list(root, index_col=None, index_values=()):
    cst_pki = get_cst_pki(con=DB_ENGINE, schemas=DB_SCHEMAS_INFO['schema'].tolist())
    cst_pki = cst_pki[
        (cst_pki['TABLE_NAME'] == root) &
        (cst_pki['CONSTRAINT_NAME'] == 'PRIMARY')
    ]
    tree = Tree(con=DB_ENGINE, tables=TABLES, root=root, cst_pki=cst_pki)
    dtree = DataTree(tree=tree)
    dtree.from_sql(
        index_col=index_col,
        index_values=index_values
    )

    data = dtree.data

    for col in dtree.table.cols.values():
        if col.col_name == dtree.pk:
            data[dtree.pk] = data.index
        f = col.web_detail_format
        data[col.col_name] = data[col.col_name].apply(lambda x: udf_format(x, f))
    data.reset_index(drop=True, inplace=True)
    data.sort_values(by=tree.pk, ascending=False, inplace=True)

    web_list_cols = sorted(
        [
            col for col in dtree.table.cols.values()
            if not pd.isna(col.web_list_order)
        ],
        key=lambda x: x.web_list_order
    )
    res = {
        'cols': [
            {
                'name': col.col_name,
                'web_label': col.label,
                'data_type': col.data_type,
                'web_obj': col.web_obj
            }
            for col in web_list_cols
        ],
        'rows': data[[col.col_name for col in web_list_cols]].to_dict(orient='records')
    }

    return res


def get_data_trees(root, index_col=None, index_values=()):

    tree = Tree(con=DB_ENGINE, tables=TABLES, root=root)
    dtree = DataTree(tree=tree)
    dtree.from_sql(
        index_col=index_col,
        index_values=index_values
    )

    return dtree


def get_tree_row(root, index_col, index_values):
    data_tree = DataTree(root=root, con=DB_ENGINES['data'], tables=TABLES)
    data_tree.from_sql(index_col=index_col, index_values=index_values)

    return data_tree


# @profile_line_by_line
def get_nested_values(root, limit=None, offset=None):

    dtree = DataTree(root=root, con=DB_ENGINES['data'], tables=TABLES)
    dtree.from_sql(limit=limit, offset=offset)
    dtree.fill_na_with_none()
    json_obj = dtree.nested_values()
    json_obj['dataSource'][0].sort(key=lambda x: x['id'], reverse=True)

    json_str = to_json_str(json_obj=json_obj)
    json_obj = to_json_obj(json_str=json_str)
    return json_obj


def get_booking_structure(in_json_obj):
    branch = in_json_obj['branch']

    t_branch = Tree(con=DB_ENGINE, tables=TABLES)
    dtree = DataTree(con=DB_ENGINE, tables=TABLES, root=branch)

    select_values = dtree.get_parents_select_values()

    json_obj = {'field_structure': t_branch.json_obj, 'select_values': select_values}

    return json_obj


def get_edit_cols(in_json_obj):

    root = in_json_obj['root']
    index_col = 'id'
    index_value = in_json_obj[index_col]

    tree_row = get_tree_row(root=root, index_col=index_col, index_values=[index_value])

    t_branch = Tree(con=DB_ENGINES['data'], tables=TABLES, root=root)
    dtree = DataTree(tree=t_branch)

    select_values = dtree.get_parents_select_values()

    json_obj = {'field_structure': tree_row.json_obj,
                'select_values': select_values}

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
                    print()
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


def booking(in_json_obj):
    # user_name = in_json_obj['user_name']
    root = in_json_obj['data']['root']
    data = in_json_obj['data']['data']
    schema = f'{PROJECT_NAME}_data_{SYS_MODE}'

    nodes = []
    for d in data:
        node, parents = fetch_data_from_dict(d=d, tables=TABLES)
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
        table = TABLES[node_root]
        for col in df.columns:
            if table.cols[col].unique or table.cols[col].foreign_key:
                df = df.drop_duplicates(subset=df.columns)
        d_dfs[node_root] = df
    print(d_dfs)
    tree = Tree(con=DB_ENGINE, tables=TABLES, root=root)

    d_dfs = mark_child_name(d_dfs=d_dfs, tree=tree)
    d_dfs = strip_values(d_dfs=d_dfs)

    dfs_to_db(d_dfs=d_dfs, tree=tree, schema=schema)


def dfs_to_db(d_dfs, tree, schema):
    for node_root in tree.booking_sequence:
        try:
            d_dfs[node_root]
        except KeyError:
            continue

        table = tree.tables[node_root]
        df = d_dfs[node_root]
        df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        print(node_root)
        print(df)
        print('*' * 100)
        if len(df) == 0:
            continue
        df_to_db(
            df=df,
            name=node_root,
            check_cols=[
                col.col_name for col in table.cols.values()
                if col.check_pk == 1],
            if_conflict='fill_update',
            con=DB_ENGINE,
            schema=schema
        )


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


def file_upload(file, timestamped=False):
    folder = os.path.join(PATH_UPLOAD, 'project_xl')
    filename = file.filename
    split_str = filename.split('.')
    filename_base, ext = '.'.join(split_str[:-1]), split_str[-1]
    if not os.path.exists(folder):
        mkdir(folder)
    if timestamped:
        name_ele = [
            filename_base,
            dt.now().strftime('%Y%m%d_%H%M%S_%f')
        ]
    else:
        name_ele = [
            filename_base
        ]
    file_path = os.path.join(
        folder,
        '_'.join(name_ele)
    ) + '.' + ext

    file.save(dst=file_path)

    return file_path, filename


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
    timestamp = dt.now().strftime("%Y%m%d_%H%M%S_%f")
    dtree = DataTree(root=root, con=DB_ENGINES['data'], tables=TABLES)
    if row_id != "":
        dtree.from_sql(index_col='id', index_values={row_id})

    template_path = os.path.join(PATH_ROOT, 'templates', 'booking_xl_template.xlsm')
    output_folder = os.path.join(PATH_ROOT, 'output', 'booking_xl_sheet')
    mkdir(output_folder)
    output_filename = f'booking_excel-{timestamp}.xlsm'
    output_path = os.path.join(
        output_folder,
        output_filename
    )

    render_booking_xl_sheet(
        output_path=output_path,
        data_tree=dtree,
        template_path=template_path,
        con=DB_ENGINE
    )

    return {
        'file_path': output_path,
        'file_name': output_filename,
    }


def xl_sheet_to_dtree(root, file_path):
    dtree = DataTree(root=root, con=DB_ENGINES['data'], tables=TABLES)
    dfs = pd.read_excel(
        file_path,
        sheet_name=None
    )
    dtree.from_excel_booking_sheet(dfs=dfs)
    return dtree


def migrate_from_xlsx(folder):
    cst_pki = get_cst_pki(con=DB_ENGINE, schemas=DB_SCHEMAS.values())
    booking_sequence = get_booking_sequence(cst_pki=cst_pki)

    for schema_tag in DB_SCHEMA_TAGS:
        schema = DB_SCHEMAS[schema_tag]
        schema_folder = os.path.join(folder, schema)
        migrate_data_from_xl_folder(
            folder=schema_folder,
            schema=schema,
            booking_sequence=booking_sequence
        )


@profile_line_by_line
def test_tree():
    t = Tree(con=DB_ENGINE, tables=TABLES, root='project')


if __name__ == '__main__':
    folder = r'D:\projects\vision6\snapshots\db\20240411_043440_401576_test_to_prod'
    migrate_from_xlsx(folder=folder)
