from hashlib import sha1
from copy import deepcopy, copy
from datetime import datetime as dt

from helper_function.hf_string import udf_format, to_json_obj, to_json_str
from helper_function.hf_file import mkdir

from sys_init import *

from table_objs import *
from meta import get_table_objs
from tree import DataTree, Tree, get_cst_pki
from xltemplate import render_booking_xl_sheet


def get_booking_table_names():
    tables = pd.read_sql(sql=f'select * from tables', con=DB_ENGINE_ADMIN)
    tables = tables[tables['is_data_table'] == 1]
    tables.sort_values(by='web_list_index')
    res = [
        {
            'name': r['table_name'],
            'web_label': r['comment']
        }
        for i, r in tables.iterrows()
        if not pd.isna(r['web_list_index'])
    ]
    return res


def get_data_list(root, index_field=None, index_values=()):
    cst_pki = get_cst_pki()
    cst_pki = cst_pki[
        (cst_pki['TABLE_NAME'] == root) &
        (cst_pki['CONSTRAINT_NAME'] == 'PRIMARY')
    ]
    tree = Tree(root=root, cst_pki=cst_pki)
    dtree = DataTree(tree=tree)
    dtree.from_sql(
        index_field=index_field,
        index_values=index_values,
        con=DB_ENGINE
    )

    data = dtree.data

    for col in dtree.table.cols.values():
        if col.field == dtree.pk:
            data[dtree.pk] = data.index
        f = col.web_detail_format
        data[col.field] = data[col.field].apply(lambda x: udf_format(x, f))
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
                'name': col.field,
                'web_label': col.web_label,
                'data_type': col.data_type,
                'web_obj': col.web_obj
            }
            for col in web_list_cols
        ],
        'rows': data[[col.field for col in web_list_cols]].to_dict(orient='records')
    }

    return res


def get_data_trees(root, index_field=None, index_values=()):
    tree = Tree(root=root)
    dtree = DataTree(tree=tree)
    dtree.from_sql(con=DB_ENGINE, index_field=index_field, index_values=index_values)

    return dtree


def get_tree_row(root, index_field, index_values):
    data_tree = DataTree(root=root)
    data_tree.from_sql(index_field=index_field, index_values=index_values, con=DB_ENGINE)

    return data_tree


# @profile_line_by_line
def get_nested_values(root, limit=None, offset=None):

    dtree = DataTree(root=root)
    dtree.from_sql(con=DB_ENGINE, limit=limit, offset=offset)
    dtree.fill_na_with_none()
    json_obj = dtree.nested_values()
    json_obj['dataSource'][0].sort(key=lambda x: x['id'], reverse=True)

    json_str = to_json_str(json_obj=json_obj)
    json_obj = to_json_obj(json_str=json_str)
    return json_obj


def get_booking_structure(in_json_obj):
    branch = in_json_obj['branch']

    t_branch = Tree()
    dtree = DataTree(root=branch)

    select_values = dtree.get_parents_select_values(con=DB_ENGINE)

    json_obj = {'field_structure': t_branch.json_obj, 'select_values': select_values}

    return json_obj


def get_update_fields(in_json_obj):
    for key, item in in_json_obj.items():
        if key == 'root':
            root = copy(item)
        elif key == 'user_name':
            user_name = copy(item)
        else:
            index_field = copy(key)
            value = copy(item)

    tree_row = get_tree_row(root=root, index_field=index_field, index_values=[value])

    t_branch = Tree(root=root)
    dtree = DataTree(tree=t_branch)

    select_values = dtree.get_parents_select_values(con=DB_ENGINE)

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
            index_field=name_ref_col.field,
            index_values={root_df[name_ref_col.field].values[0]},
            con=DB_ENGINE
        )
        root_df['name'] = root_df.apply(
            lambda x: '-'.join([x[name_ref_col.field], tree.root, str(len(root_data_tree.data))]),
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
    naming_fields = [(col_name, col.naming_field_order)
                     for col_name, col in table.cols.items()
                     if not pd.isna(col.naming_field_order)]
    if len(naming_fields) > 0:
        naming_fields = [field for field, order in sorted(naming_fields, key=lambda x: x[1])]
        d['name'] = [{
            'name':
            '-'.join([list(d[field][0].values())[0] for field in naming_fields])
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
    user_name = in_json_obj['user_name']
    root = in_json_obj['data']['root']
    data = in_json_obj['data']['data']

    tables = get_table_objs()

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
    tree = Tree(root=root)

    d_dfs = mark_child_name(d_dfs=d_dfs, tree=tree)
    d_dfs = strip_values(d_dfs=d_dfs)

    booking_from_dfs(d_dfs=d_dfs, tree=tree)


def booking_from_dfs(d_dfs, tree):

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
        pd_to_db_check_pk(
            df=df,
            name=node_root,
            check_fields=[
                col.field for col in table.cols.values()
                if col.check_pk],
            if_conflict='fill_update',
            con=DB_ENGINE,
            schema=DB_SCHEMA
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


def file_upload(file, file_str, folder, timestamped=False):
    filename = file.filename
    split_str = filename.split('.')
    filename_base, ext = '.'.join(split_str[:-1]), split_str[-1]
    sha_name = sha1(file_str).hexdigest()
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
    dtree = DataTree(root=root)
    if row_id != "":
        dtree.from_sql(index_field='id', index_values={row_id}, con=DB_ENGINE)

    template_path = os.path.join(PATH_ROOT, 'core', 'xltemplate.xlsm')
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


def booking_from_xl_sheet(root, file_path):
    dtree = DataTree(root=root)
    dfs = pd.read_excel(
        file_path,
        sheet_name=None
    )
    dtree.from_excel_booking_sheet(dfs=dfs)
    booking_from_dfs(d_dfs=dtree.relevant_data_set, tree=dtree)

    status = 0

    return status
