import sys
import os

import subprocess

from sqlalchemy.orm import sessionmaker

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.helper_function.hf_data import is_equal
from mint.sys_init import *
from mint.db.tree import DataTree, Tree
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


def json_to_dfs(values):
    dfs = {
        table_root: pd.DataFrame(
            data=data, columns=[col.col_name for col in TABLES[table_root].cols]
        )
        for table_root, data in values.items()
    }
    return dfs


def create_tree(jo):
    submit_values = jo['submitValues']
    root = jo['root']

    dfs = json_to_dfs(submit_values)

    engine = get_engine('data', auto_commit=False)
    con = engine.connect()
    dtree = DataTree(root=root, con=con, tables=TABLES)
    dtree.from_relevant_data_set(dfs)
    trimmed_relevant_data_set = dtree.relevant_data_set

    Session = sessionmaker(bind=engine)
    session = Session()

    error_message = None
    with engine.begin():
        try:
            for root in dtree.booking_sequence:
                df = trimmed_relevant_data_set[root]
                print('-' * 50)
                print(root)
                print('-' * 50)
                print(df)
                print('*' * 100)
                df.to_sql(name=root, con=con, if_exists='append', index=False)
            session.commit()
        except SQLAlchemyError as e:
            # 1048 缺失值
            # 1452 引用外键约束
            session.rollback()
            print('发生错误，事务已回滚 ', e)
            raise e
        finally:
            session.close()

    con.close()
    engine.dispose()
    return error_message


def delete(
        con,
        root,
        df: pd.DataFrame,
        index_col=None,
        index_values=None,
):
    print(
        f'[sql] deleting from {root} where `{index_col}` in ({", ".join(map(str, index_values))})')
    if index_col is None:
        print('No index column specified. nothing happens.')
        return
    if index_values is None:
        print('No index values specified. nothing happens.')
        return

    index = df[index_col].tolist()
    sql = f'delete from {root} where `{index_col}` in ({", ".join(map(str, index))})'
    con.execute(text(sql))


def delete_tree(jo):
    root = jo['root']
    index_col = jo['indexCol']
    index_value = jo['indexValue']
    preview = jo['preview']

    if index_value is None:
        return

    con = get_con('data')
    dtree = DataTree(root=root, con=con, tables=TABLES)
    dtree.from_sql(index_col=index_col, index_values={index_value})

    relevant_data_set = dtree.relevant_data_set

    preview_data = []
    bs = dtree.all_childhood_names()
    bs = [root] + list(bs)
    for table_root in bs:
        try:
            df = relevant_data_set[table_root].reset_index()
        except KeyError:
            continue
        col_objs = [col for col in TABLES[table_root].cols
                if col.web_visible == 1 or
                   col.col_name == 'id']
        preview_data.append({
            'label': TABLES[table_root].label,
            'headers': [{
                'label': col.label,
                'key': col.col_name
            } for col in col_objs
            ],
            'data': df.to_dict(orient='records')
        })
    if preview:
        con.close()
        return preview_data
    else:
        df = relevant_data_set[root].reset_index()
        delete(con, root, df, index_col=index_col, index_values=[index_value])
        con.close()


def update_tree(jo):
    root = jo['root']
    index_col = jo['indexCol']
    index_values = jo['indexValues']
    submit_values = json_to_dfs(jo['submitValues'])

    engine = get_engine('data', auto_commit=False)
    con = engine.connect()

    dtree = DataTree(con=con, root=root, tables=TABLES)
    dtree.from_sql(index_col=index_col, index_values=index_values)
    prev_values = {
        table_root: df.replace(np.nan, None).reset_index().to_dict(
            orient='records')
        for table_root, df in dtree.relevant_data_set.items()
    }

    res = check_update_all(
        prev_values=prev_values,
        submit_values=submit_values,
        childhood_table_names=dtree.all_childhood_names()
    )

    sqls = []
    for table_name in dtree.booking_sequence:
        if table_name in res:
            updates = res[table_name]
            for update in updates:
                if update[0] == 'insert':
                    cols = [
                        key.replace('__new', '') for key in update[1].keys()
                    ]
                    sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(
                        table_name,
                        ', '.join(cols),
                        ', '.join([':{}'.format(col) for col in cols]),
                    )
                    sql = text(sql)
                    params = {
                        col: update[1][col + '__new'] for col in cols
                    }
                elif update[0] == 'delete':
                    sql = 'DELETE FROM `{}` WHERE id = :id'.format(table_name)
                    sql = text(sql)
                    params = {'id': int(update[1]['id'])}
                elif update[0] == 'update':
                    sql = 'UPDATE `{}` SET {} WHERE id = :id'.format(
                        table_name,
                        ', '.join([
                            '{} = :{}'.format(col1, col2)
                            for col1, col2 in zip(
                                list(update[1].keys()),
                                list(update[2].keys())
                            )
                        ]))
                    sql = text(sql)
                    params = {**update[2], 'id': int(update[1]['id'])}
                else:
                    raise ValueError('Unknown update type: {}'.format(update[0]))
                sqls.append((sql, params))

    Session = sessionmaker(bind=engine)
    session = Session()

    sql = ''
    params = {}
    try:
        for sql, params in sqls:
            print('*' * 100)
            print(sql)
            print(params)
            print('*' * 100)
            con.execute(sql, params)
    except SQLAlchemyError as e:
        # 1048 缺失值
        # 1452 引用外键约束
        print(sql)
        print(params)
        session.rollback()
        print('发生错误，事务已回滚 ', e)
        session.close()
        con.close()
        engine.dispose()
        return str(e)

    con.commit()
    session.close()
    con.close()
    engine.dispose()

    return res


def get_submit_preview_tables(jo):
    root = jo['root']
    index_col = jo['indexCol']
    index_values = jo['indexValues']
    submit_values = json_to_dfs(jo['submitValues'])
    con = get_con('data')

    dtree = DataTree(con=con, root=root, tables=TABLES)
    dtree.from_sql(index_col=index_col, index_values=index_values)
    prev_values = {
        table_root: df.replace(np.nan, None).reset_index().to_dict(
            orient='records')
        for table_root, df in dtree.relevant_data_set.items()
    }

    res = check_update_all(
        prev_values=prev_values,
        submit_values=submit_values,
        childhood_table_names=dtree.all_childhood_names()
    )
    preview_tables = []
    for table_name in dtree.booking_sequence:
        if table_name in res:
            table_props = {
                'label': TABLES[table_name].label,
                'headers': [
                    {'key': col.col_name, 'label': col.label}
                    for col in TABLES[table_name].cols
                    if col.web_visible == 1
                ],
                'data': []
            }

            updates = res[table_name]
            for update in updates:
                if update[0] == 'insert':
                    update_preview_row = {
                        header['key']: (
                            '[insert]',
                            update[1][header['key'] + '__new']
                        )
                        for header in table_props['headers']
                    }
                elif update[0] == 'delete':
                    update_preview_row = {
                        header['key']: (
                            update[1][header['key']],
                            '[delete]'
                        )
                        for header in table_props['headers']
                    }
                elif update[0] == 'update':
                    update_preview_row = {
                        header['key']: (
                            update[1][header['key']],
                            update[2][header['key'] + '__new']
                        )
                        for header in table_props['headers']
                    }
                else:
                    raise ValueError(
                        'Unknown update type: {}'.format(update[0]))
                table_props['data'].append(update_preview_row)
            preview_tables.append(table_props)
    return preview_tables


def check_update(jo):
    col_name = jo['colName']
    dir_table_name = jo['dirTableName']
    value = jo['value']
    row_id = jo['rowId']
    con = get_con('data')
    sql = 'SELECT `{}` FROM `{}` WHERE id = :row_id'.format(
        col_name,
        dir_table_name,
    )
    result = con.execute(text(sql), {'value': value, 'row_id': row_id})
    con.close()
    value_in_db = result.fetchone()
    res = None
    if value_in_db is not None and not is_equal(value_in_db[0], value):
        if value_in_db[0] is None:
            s = '[空值]'
        else:
            s = value_in_db[0]
        if value is None:
            value = '[空值]'
        res = '{} -> {}'.format(s, value)
    return res


def check_update_all(prev_values, submit_values, childhood_table_names):

    table_names = list(submit_values.keys())
    res = {}
    for table_name in table_names:
        table_res = []
        try:
            pdf = pd.DataFrame(prev_values[table_name])
            # pdf = pdf[[
            #     col.col_name for col in TABLES[table_name].cols
            #     if col.col_name == 'id' or col.web_visible == 1
            # ]]
        except KeyError:
            pdf = pd.DataFrame(columns=[
                col.col_name for col in TABLES[table_name].cols
                # if col.col_name == 'id' or col.web_visible == 1
            ])
        try:
            vdf = pd.DataFrame(submit_values[table_name])
            # vdf = vdf[[
            #     col.col_name for col in TABLES[table_name].cols
            #     if col.col_name == 'id' or col.web_visible == 1
            # ]]
        except KeyError:
            vdf = pd.DataFrame(
                columns=[
                    col.col_name for col in TABLES[table_name].cols
                    # if col.col_name == 'id' or col.web_visible == 1
                ]
            )
        if len(pdf) == 0:
            pdf = pd.DataFrame(columns=[
                col.col_name for col in TABLES[table_name].cols
                # if col.col_name == 'id' or col.web_visible == 1
            ])
        if not 'id' in vdf.columns.tolist():
            vdf['id'] = np.nan

        cols = vdf.columns.tolist()
        cols_new = [col + '__new' for col in cols]

        vdf['id__new'] = vdf['id']

        compare = pd.merge(
            left=pdf.replace({None: np.nan}),
            right=vdf.replace({None: np.nan}),
            how='outer',
            on='id',
            suffixes=('', '__new')
        ).replace(np.nan, None)

        for r, row in compare.iterrows():
            if row['id'] is None and row['id__new'] is None:
                table_res.append(
                    ('insert', row[cols_new].to_dict())
                )

            elif (row['id'] is not None
                  and all([
                        row[col] is None for col in row.keys() if
                        col.endswith('__new')
                    ])
                and table_name in childhood_table_names
            ):
                table_res.append(
                    ('delete', row[cols].to_dict())
                )
            elif (row['id'] is not None and row['id__new'] is not None and
                  row['id'] == row['id__new']):
                table_res.append(
                    ('update', row[cols].to_dict(), row[cols_new].to_dict())
                )
            else:
                print(table_name)
                print(row)
                print(compare)
                raise ValueError('Unknown case')

        if len(table_res) > 0:
            res[table_name] = table_res
    return res


def check_mysqldump_installed():
    try:
        # 运行 `mysqldump --version` 来检查是否安装
        result = subprocess.run(['mysqldump', '--version'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            print("mysqldump 已安装。版本信息：")
            print(result.stdout)
        else:
            print("mysqldump 未安装，或未添加到系统的 PATH 环境变量。")
    except FileNotFoundError:
        print(
            "未找到 mysqldump。请确保 mysqldump 已正确安装并且已添加到 PATH 环境变量。")


if __name__ == "__main__":
    check_mysqldump_installed()
