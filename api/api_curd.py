import sys
import os

import subprocess


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.helper_function.hf_data import is_equal
from mint.sys_init import *
from mint.db.tree import DataTree
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from typing import Dict


def create(
        con,
        root,
        df: pd.DataFrame,
        skip_dups=True
):
    try:
        df.to_sql(root, con=con, if_exists='append', index=False)
    except IntegrityError as e:
        if e.args[0] == 1062:
            if skip_dups:
                for i, row in df.iterrows():
                    try:
                        df.loc[i].to_sql(root, con=con, if_exists='append', index=False)
                    except IntegrityError as e:
                        pass
            else:
                raise e


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
        con,
        root,
        index_col=None,
        index_values=None,
        submit_values: Dict[str, pd.DataFrame] = None,
        change_table=False,
        preview=True,
        **kwargs
):

    dtree = DataTree(con=con, root=root, tables=TABLES)
    dtree.from_sql(index_col=index_col, index_values=index_values)
    prev_values_set = {}
    for table_root, df in dtree.relevant_data_set.items():
        prev_values_set[table_root] = df.replace(np.nan, None)

    return update_tree_exec(
        con=con,
        dtree=dtree,
        prev_values_set=prev_values_set,
        submit_values=submit_values,
        change_table=change_table,
        preview=preview,
        **kwargs
    )


def update_tree_exec(
        con,
        dtree,
        prev_values_set,
        submit_values: Dict[str, pd.DataFrame] = None,
        change_table=False,
        preview=True,
        **kwargs
):

    table_changes = {}
    for table_root, df in submit_values.items():
        visible_cols = [col for col in TABLES[table_root].cols if col.web_visible == 1 or col.col_name == 'id']
        prev_values = prev_values_set[table_root].reset_index().to_dict(orient='records')
        cur_values = submit_values[table_root]
        row_changes = []

        if change_table:
            cur_values = [cur_row for cur_row in cur_values if cur_row['_block'] == 'new']
            for i, cur_row in enumerate(cur_values):
                value_changes = {}
                for col in visible_cols:
                    col_name = col.col_name
                    pv = '[add]'
                    cv = cur_row[col_name]
                    value_changes[col_name] = (pv, cv)
                row_changes.append(value_changes)
        else:
            if len(prev_values) >= len(cur_values):
                for i, cur_row in enumerate(cur_values):
                    value_changes = {}
                    prev_row = prev_values[i]
                    for col in visible_cols:
                        col_name = col.col_name
                        pv = prev_row[col_name]
                        try:
                            cv = cur_row[col_name]
                        except KeyError:
                            cv = None
                        value_changes[col_name] = (pv, cv)
                    row_changes.append(value_changes)

                for i, prev_row in enumerate(prev_values[len(cur_values):]):
                    value_changes = {}
                    for col in visible_cols:
                        col_name = col.col_name
                        pv = prev_row[col_name]
                        cv = '[delete]'
                        value_changes[col_name] = (pv, cv)
                    row_changes.append(value_changes)
            else:
                for i, prev_row in enumerate(prev_values):
                    value_changes = {}
                    cur_row = cur_values[i]
                    for col in visible_cols:
                        col_name = col.col_name
                        pv = prev_row[col_name]
                        try:
                            cv = cur_row[col_name]
                        except KeyError:
                            cv = None
                        value_changes[col_name] = (pv, cv)
                    row_changes.append(value_changes)

                for i, cur_row in enumerate(cur_values[len(prev_values):]):
                    value_changes = {}
                    for col in visible_cols:
                        col_name = col.col_name
                        pv = '[add]'
                        try:
                            cv = cur_row[col_name]
                        except KeyError:
                            cv = None
                        value_changes[col_name] = (pv, cv)
                    row_changes.append(value_changes)

        table_changes[table_root] = row_changes

    if preview:
        preview_tables = []
        for table_root, row_changes in table_changes.items():
            visible_cols = [col for col in TABLES[table_root].cols if col.web_visible == 1]
            if table_root in table_changes:
                row_changes = table_changes[table_root]
                for row_change in row_changes:
                    if any([
                        not is_equal(item[1][0], item[1][1])
                        for item in row_change.items()
                        if item[0] != 'id'
                    ]):
                        break
                else:
                    continue

            change_data = []
            for row_change in row_changes:
                value_changes = {
                    col.col_name: f'{row_change[col.col_name][0]} -> {row_change[col.col_name][1]}'
                    if not is_equal(row_change[col.col_name][0], row_change[col.col_name][1])
                    else row_change[col.col_name][1]
                    for col in visible_cols
                    if col.col_name != 'id'
                }
                change_data.append(value_changes)

            df = pd.DataFrame(data=change_data).replace(np.nan, None)
            df = df.rename(columns={col.col_name: col.label for col in visible_cols})
            preview_tables.append({
                'label': TABLES[table_root].label,
                'data': df.to_dict(orient='split')
            })
        return preview_tables
    else:
        sqls = []
        for table_root in dtree.booking_sequence:
            if table_root in table_changes:
                row_changes = table_changes[table_root]
                for row_change in row_changes:
                    if any([
                        not is_equal(item[1][0], item[1][1])
                        for item in row_change.items()
                        if item[0] != 'id'
                    ]):
                        break
                else:
                    continue

                if table_root in dtree.all_childhood_names():
                    prev_ids = [
                        str(row['id']) for row in
                        prev_values_set[table_root].reset_index().to_dict(orient='records')
                    ]
                    if len(prev_ids) > 0:
                        prev_ids = ', '.join(prev_ids)
                        sql = f"delete from {table_root} where id in ({prev_ids})"
                        sqls.append((text(sql), {}))

                    for row_change in row_changes:
                        row = {col: row_change[col][1] for col in row_change.keys()}
                        if table_root in [c.root for c in dtree.children]:
                            ref = dtree.c[table_root].ref
                            reffed = dtree.c[table_root].reffed
                            reffed_values = dtree.data[reffed].values[0]
                            row[ref] = reffed_values
                        sql = f"insert into {table_root} ({', '.join(row.keys())}) values ({', '.join([f':{v}' for v in row.keys()])})"
                        sqls.append((text(sql), row))
                else:
                    for row_change in row_changes:
                        if row_change['id'][1] == '[delete]':
                            row_id = row_change['id'][0]
                            sql = f"delete from {table_root} where id = {row_id}"
                            sqls.append((text(sql), {}))
                        elif row_change['id'][0] == '[add]':
                            row = {col: row_change[col][1] for col in row_change.keys() if col != 'id'}
                            sql = f"insert into {table_root} ({', '.join(row.keys())}) values ({', '.join([f':{v}' for v in row.keys()])})"
                            sqls.append((text(sql), row))
                        else:
                            row_id = row_change['id'][0]
                            row = {col: row_change[col][1] for col in row_change.keys()}
                            if len(row) > 0:
                                sql = f"update {table_root} set {', '.join([f'{col} = :{col}' for col in row.keys()])} where id = {row_id}"
                                sqls.append((text(sql), row))

        for item in sqls:
            print(item[0].text)
            con.execute(*item)
        con.commit()


def check_mysqldump_installed():
    try:
        # 运行 `mysqldump --version` 来检查是否安装
        result = subprocess.run(['mysqldump', '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            print("mysqldump 已安装。版本信息：")
            print(result.stdout)
        else:
            print("mysqldump 未安装，或未添加到系统的 PATH 环境变量。")
    except FileNotFoundError:
        print("未找到 mysqldump。请确保 mysqldump 已正确安装并且已添加到 PATH 环境变量。")

if __name__ == "__main__":
    check_mysqldump_installed()