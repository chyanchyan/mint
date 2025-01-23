import shutil
import os
import sys
from sqlalchemy import text


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

import mint.globals as glb
from mint.globals import *
from mint.db.utils import *
from mint.helper_function.hf_string import to_json_str, list_to_attr_code
from mint.helper_function.wrappers import sub_wrapper
from mint.meta.table_objs import get_tables_from_info
from mint.meta import models


def get_schema(schema_tag):
    res = db_get_schema(schema_tag=schema_tag, sys_mode=SYS_MODE, project_name=PROJECT_NAME)
    return res


def get_con(schema_tag=None, auto_commit=True):
    return get_engine_con_url(schema_tag=schema_tag, auto_commit=auto_commit)[1]

def get_engine(schema_tag=None, auto_commit=True):
    return get_engine_con_url(schema_tag=schema_tag, auto_commit=auto_commit)[0]


def get_fg_data_con():
    host = DB_PARAMS['db_fg_host']
    port = DB_PARAMS['db_fg_port']
    username = DB_PARAMS['db_fg_username']
    password = DB_PARAMS['db_fg_password']
    schema = 'soams_dxm'
    charset = DB_PARAMS['db_fg_charset']
    url = str(
        f'mysql+pymysql://{username}:{password}@{host}:{port}/'
        f'{schema}?charset={charset}&autocommit=true'
    )
    engine = create_engine(url=url)
    con = engine.connect()
    return con


def get_engine_con_url(schema_tag=None, auto_commit=True):
    if schema_tag is None:
        return db_connect_db(
            **DB_PARAMS,
            schema='',
            auto_commit=auto_commit
        )
    else:
        return db_connect_db(
            **DB_PARAMS,
            schema=get_schema(schema_tag=schema_tag),
            auto_commit=auto_commit
        )


def get_schema_tags():
    con_core = get_con('core')
    schema_tags = db_get_schema_tags(con=con_core)
    con_core.close()
    return schema_tags


def refresh_table_info_to_db():
    con = get_con('core')
    table_info_path = os.path.join(PATH_PROJECT, 'table_info.xlsx')
    if not os.path.exists(table_info_path):
        table_info_path = os.path.join(PATH_ROOT, 'table_info.xlsx')
        if not os.path.exists(table_info_path):
            shutil.copy(
                src=os.path.join(PATH_ROOT, 'templates', 'table_info_template.xlsx'),
                dst=table_info_path
            )

    schemas = pd.read_excel(
        table_info_path,
        sheet_name='schemas'
    )
    schemas.to_sql(
        name='schemas', con=con, if_exists='replace', index=False
    )

    tables = pd.read_excel(
        table_info_path,
        sheet_name='tables'
    )
    tables.to_sql(
        name='tables', con=con, if_exists='replace', index=False
    )

    cols = pd.read_excel(
        table_info_path,
        sheet_name='cols'
    )
    cols.to_sql(
        name='cols', con=con, if_exists='replace', index=False
    )
    return schemas, tables, cols


@sub_wrapper(SYS_MODE)
def refresh_db_info(con):
    global DB_SCHEMAS_INFO
    global DB_TABLES_INFO
    global DB_COLS_INFO

    DB_SCHEMAS_INFO = pd.read_sql(sql=f'select * from `schemas`', con=con)
    DB_TABLES_INFO = pd.read_sql(sql=f'select * from tables', con=con)
    DB_COLS_INFO = pd.read_sql(sql=f'select * from cols', con=con)
    DB_SCHEMAS_INFO['schema'] = DB_SCHEMAS_INFO['schema_tag'].apply(
        lambda x: get_schema(x)
    )


@sub_wrapper(SYS_MODE)
def refresh_table_obj():
    template_pth = os.path.join(PATH_ROOT, 'meta', 'templates', 'table_objs.py')
    output_pth = os.path.join(PATH_ROOT, 'meta', 'table_objs.py')
    code = open(template_pth, 'r').readlines()

    st_mark = ' ' * 8 + '# col attr start\n'
    ed_mark = ' ' * 8 + '# col attr end\n'
    code = list_to_attr_code(
        code_template=code,
        attr_list=DB_COLS_INFO.columns.to_list(),
        df_var_name='col_info',
        st_mark=st_mark,
        ed_mark=ed_mark,
        intent_blocks=2
    )
    st_mark = ' ' * 12 + '# table attr start\n'
    ed_mark = ' ' * 12 + '# table attr end\n'
    code = list_to_attr_code(
        code_template=code,
        attr_list=DB_TABLES_INFO.columns.to_list(),
        df_var_name='table_info',
        st_mark=st_mark,
        ed_mark=ed_mark,
        intent_blocks=3
    )
    f = open(output_pth, 'w')
    f.writelines(code)
    f.close()


@sub_wrapper(SYS_MODE)
def refresh_models():
    # load models file template
    template_pth = os.path.join(PATH_ROOT, 'meta', 'templates', 'models.py')
    output_pth = os.path.join(PATH_ROOT, 'meta', 'models.py')
    f = open(template_pth, encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    tables = get_tables_from_info(
        tables_info=DB_TABLES_INFO,
        cols_info=DB_COLS_INFO,
        get_schema=get_schema
    )
    tables_list = [v for k, v in tables.items()]
    tables_list.sort(key=lambda x: x.order)
    table_blocks = []
    for table in tables_list:
        table_blocks.append(table.to_model_code())
        table_blocks.extend(['\n', '\n'])

    res = pre_block + table_blocks + post_block

    # write to file
    f = open(output_pth, encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


@sub_wrapper(SYS_MODE)
def create_tables():
    engine, con, url = get_engine_con_url('data')
    engine = create_engine(url)
    print("creating tables")
    while True:
        try:
            models.Base.metadata.create_all(engine)
            break
        except sqlalchemy.exc.OperationalError as e:
            if e.orig.args[0] == 1049:
                schema = e.orig.args[1].split("'")[1]
                print(f'schema "{schema}" doesnt exist.')
                print('creating...')
                con.execute(text(f'create schema {schema}'))
                print(f'schema "{schema}" created')
            else:
                raise e
    print("tables created")
    con.close()

DB_SCHEMAS_INFO, DB_TABLES_INFO, DB_COLS_INFO = refresh_table_info_to_db()
refresh_table_obj()
refresh_models()
create_tables()
TABLES = get_tables('data')
print(f'host name: {HOST_NAME}')

