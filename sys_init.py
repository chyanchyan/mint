import shutil
import os
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.api import globals as glb
from mint.api.globals import *
from mint.api.db import *
from mint.helper_function.hf_string import to_json_str


def get_schema(schema_tag):
    return db_get_schema(schema_tag=schema_tag, sys_mode=SYS_MODE, project_name=PROJECT_NAME)


def get_con(schema_tag=None):
    if schema_tag is None:
        return db_connect_db(
            **DB_PARAMS,
            schema=''
        )[1]
    else:
        return db_connect_db(
            **DB_PARAMS,
            schema=get_schema(schema_tag=schema_tag)
        )[1]


def get_engine_con_url(schema_tag=None):
    if schema_tag is None:
        return db_connect_db(
            **DB_PARAMS,
            schema=''
        )
    else:
        return db_connect_db(
            **DB_PARAMS,
            schema=get_schema(schema_tag=schema_tag)
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


DB_SCHEMAS_INFO, DB_TABLES_INFO, DB_COLS_INFO = refresh_table_info_to_db()


if __name__ == '__main__':
    gs = dict(globals()['glb'].__dict__)
    gs_ = copy(gs)
    for key in gs:
        if key[:2] == '__':
            gs_.pop(key)
    print(to_json_str(gs_))
