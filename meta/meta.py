import os.path
from datetime import datetime as dt
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, create_engine

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.sys_init import *
from mint.helper_function.wrappers import sub_wrapper
from mint.helper_function.hf_file import snapshot, mkdir
from mint.helper_function.hf_string import list_to_attr_code
from mint.helper_function.hf_db import export_xl


@sub_wrapper(SYS_MODE)
def drop_schemas(schema_tags=None):

    engine, con, url = get_engine_con_url()
    session_class = sessionmaker(bind=engine)
    session = session_class()

    if schema_tags is None:
        schema_tags = get_schema_tags()

    for schema_tag in schema_tags:
        schema = get_schema(schema_tag=schema_tag)
        drop_schema(session=session, schema=schema)
    session.commit()
    session.close()
    con.close()


@sub_wrapper(SYS_MODE)
def drop_schema(session, schema):
    session.execute(
        text(f'drop database if exists {schema}')
    )
    session.execute(
        text(f'create database {schema}')
    )


@sub_wrapper(SYS_MODE)
def snapshot_table_obj():
    snapshot(src_path='table_objs.py', dst_folder=PATH_META_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def snapshot_models():
    snapshot(src_path='models.py', dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def refresh_table_obj():
    template_pth = 'templates/table_objs_template.py'
    output_pth = 'table_objs.py'
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
    f = open('templates/models_template.py', encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    if 'mint' in __name__.split('.'):
        exec('from .meta.table_objs import get_tables')
    else:
        exec('from mint.meta.table_objs import get_tables')
    tables = eval('get_tables(tables_info=DB_TABLES_INFO, cols_info=DB_COLS_INFO)')
    tables_list = [v for k, v in tables.items()]
    tables_list.sort(key=lambda x: x.order)
    table_blocks = []
    for table in tables_list:
        table_blocks.append(table.to_model_code())
        table_blocks.extend(['\n', '\n'])

    res = pre_block + table_blocks + post_block

    # write to file
    f = open('models.py', encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


def snapshot_database(schema_tags=None, comments=''):
    if schema_tags is None:
        schema_tags = get_schema_tags()
    folder = dt.now().strftime('%Y%m%d_%H%M%S_%f') + f'-{comments}'
    if not os.path.exists(PATH_DB_SNAPSHOT):
        mkdir(PATH_DB_SNAPSHOT)

    con = get_con()

    for schema_tag in schema_tags:
        folder = os.path.join(
            str(PATH_DB_SNAPSHOT),
            str(folder),
            str(get_schema(schema_tag))
        )
        if not os.path.exists(folder):
            mkdir(folder)

        export_xl(
            output_folder=folder,
            con=con,
            schema=get_schema(schema_tag=schema_tag),
            table_names=None
        )


@sub_wrapper(SYS_MODE)
def create_tables():
    if 'mint' in __name__.split('.'):
        exec('from .meta import models')
    else:
        exec('from mint.meta import models')
    engine, con, url = get_engine_con_url()
    engine = create_engine(url)
    con.close()
    print("creating tables")
    exec('models.Base.metadata.create_all(engine)')
    print("tables created")


@sub_wrapper(SYS_MODE)
def restore_sys():

    drop_schemas()
    refresh_table_info_to_db()

    con = get_con('core')
    refresh_db_info(con=con)
    con.close()

    snapshot_table_obj()
    refresh_table_obj()
    snapshot_models()
    refresh_models()
    create_tables()


@sub_wrapper(SYS_MODE)
def restore_table():
    refresh_table_info_to_db()

    con = get_con('core')
    refresh_db_info(con=con)
    con.close()

    snapshot_table_obj()
    refresh_table_obj()


@sub_wrapper(SYS_MODE)
def restore_db(schema_tags=None):
    drop_schemas(schema_tags)
    create_tables()


@sub_wrapper(SYS_MODE)
def add_table():
    refresh_table_info_to_db()

    con = get_con('core')
    refresh_db_info(con=con)
    con.close()


    snapshot_models()
    refresh_models()
    create_tables()


def init_sys():
    refresh_table_info_to_db()
    refresh_models()
    refresh_table_obj()


if not os.path.exists('table_obj.py'):
    refresh_table_obj()

if not os.path.exists('models.py'):
    refresh_models()
