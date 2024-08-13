import os.path
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
