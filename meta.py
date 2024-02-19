import os.path
import re

from sys_init import *
from table_objs import MetaTable
from helper_function.wrappers import sub_wrapper
from helper_function.hf_file import snapshot

from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, create_engine


def list_to_col_attr_code(
        code_template,
        attr_list,
        df_var_name,
        st_mark,
        ed_mark,
        intent_blocks
):
    attr_code_str_base = " " * 4 * intent_blocks + f"self.%s = {df_var_name}['%s']\n"
    st_index = code_template.index(st_mark)
    ed_index = code_template.index(ed_mark)
    insert_code = [attr_code_str_base % (attr, attr) for attr in attr_list]
    res_code = code_template[: st_index + 1] + insert_code + code_template[ed_index:]
    return res_code


def attr_code_to_list(
        col_attr_code,
        st_mark,
        ed_mark,
):
    st_index = col_attr_code.index(st_mark)
    ed_index = col_attr_code.index(ed_mark)
    lines = col_attr_code[st_index + 1: ed_index]
    res = []
    for line in lines:
        match = re.match(
            r"\s*self\.([a-zA-Z_]+) = ",
            line
        )
        if match:
            field_name = match.group(1)
            res.append(field_name)
            print(field_name)
        else:
            print(line, 'not found')

    return res


def get_table_objs(schema='data'):
    res = {}
    table_names = DB_TABLES_INFO[DB_TABLES_INFO['schema'] == schema]['table_name'].tolist()

    for i, table_r in DB_TABLES_INFO.iterrows():
        table_name = table_r['table_name']
        if table_name not in table_names and not pd.isna(table_r['schema']):
            continue

        if not pd.isna(table_r['ancestors']):
            ancestors = [a.strip() for a in table_r['ancestors'].split(',')]
        else:
            ancestors = []

        ancestors_copy = list(ancestors)
        base_ancestor_tables = []
        while len(ancestors_copy) > 0:
            ancestor = ancestors_copy.pop()
            ancestor_row = DB_TABLES_INFO[DB_TABLES_INFO['table_name'] == ancestor]

            if not pd.isna(ancestor_row['ancestors'].values[0]):
                ancestor_ancestors = [
                    a.strip()
                    for a in ancestor_row['ancestors'].values[0].split(',')
                ]
                ancestors_copy.extend(ancestor_ancestors)

            base_ancestor_tables.append(ancestor)

        table_cols_info = DB_COLS_INFO[
            (DB_COLS_INFO['table_name'] == table_name) |
            DB_COLS_INFO['table_name'].isin(base_ancestor_tables)
        ]

        res[table_r['table_name']] = MetaTable(
            table_info=table_r,
            cols_info=table_cols_info,
            order=i
        )

    return res


def refresh_model_file(schema):

    # load models file template
    f = open('models_template.py', encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    tables = get_table_objs(schema=schema)
    tables_list = [v for k, v in tables.items()]
    tables_list.sort(key=lambda x: x.order)
    table_blocks = []
    for table in tables_list:
        table_blocks.append(table.to_model_code())
        table_blocks.extend(['\n', '\n'])

    res = pre_block + table_blocks + post_block

    # write to file
    f = open(f'models_{schema}.py', encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


def create_tables(schema):
    exec(f'from models_{schema} import Base')
    print(f'creating tables in {schema}')
    exec(f'Base.metadata.create_all(DB_ENGINE_{str.upper(schema)})')
    print(f'creating tables in {schema} done')


@sub_wrapper
def snapshot_models():
    model_file_path = os.path.join('models.py')
    snapshot(src_path=model_file_path, dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper
def snapshot_table_obj():
    meta_file_path = os.path.join('table_objs.py')
    snapshot(src_path=meta_file_path, dst_folder=PATH_META_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper
def refresh_models():
    schemas = set(DB_TABLES_INFO[~pd.isna(DB_TABLES_INFO['schema'])]['schema'].tolist())
    for schema in schemas:
        refresh_model_file(schema=schema)


@sub_wrapper
def create_all_schema_tables():
    schemas = set(DB_TABLES_INFO[~pd.isna(DB_TABLES_INFO['schema'])]['schema'].tolist())
    for schema in schemas:
        create_tables(schema=schema)


@sub_wrapper
def refresh_table_obj():
    snapshot_table_obj()
    code = open('table_objs_template.py', 'r').readlines()

    st_mark = ' ' * 8 + '# col attr start\n'
    ed_mark = ' ' * 8 + '# col attr end\n'
    code = list_to_col_attr_code(
        code_template=code,
        attr_list=DB_COLS_INFO.columns.to_list(),
        df_var_name='col_info',
        st_mark=st_mark,
        ed_mark=ed_mark,
        intent_blocks=2
    )
    st_mark = ' ' * 12 + '# table attr start\n'
    ed_mark = ' ' * 12 + '# table attr end\n'
    code = list_to_col_attr_code(
        code_template=code,
        attr_list=DB_TABLES_INFO.columns.to_list(),
        df_var_name='table_info',
        st_mark=st_mark,
        ed_mark=ed_mark,
        intent_blocks=3
    )
    f = open('table_objs.py', 'w')
    f.writelines(code)
    f.close()


def restore_data_db():

    session_class = sessionmaker(bind=DB_ENGINE_DATA)
    session = session_class()

    session.execute(
        text(f'drop database if exists {DB_SCHEMA_DATA}')
    )
    session.execute(
        text(f'create database {DB_SCHEMA_DATA}')
    )
    session.commit()
    session.close()

    from models_data import Base
    engine, con = connect_db(
        db_type=DB_TYPE,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        schema=DB_SCHEMA_DATA,
        charset=DB_CHARSET
    )
    print('creating tables')
    Base.metadata.create_all(engine)
    print('tables created')


if __name__ == '__main__':
    restore_data_db()
    pass