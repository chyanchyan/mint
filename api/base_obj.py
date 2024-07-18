import pandas as pd

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.api.globals import *
from mint.api.db import db_connect_db, db_get_schema
from mint.meta.table_objs import get_tables_from_info
from mint.helper_function.hf_func import profile_line_by_line


def get_con(schema_tag):
    engine, con, url = db_connect_db(
        **DB_PARAMS,
        schema=db_get_schema(
            schema_tag=schema_tag,
            sys_mode=SYS_MODE,
            project_name=PROJECT_NAME
        )
    )
    return con


def get_tables(schema_tag: str = None):
    if schema_tag is not None:
        filter_sql = f' where `schema_tag` = "{schema_tag}" or `schema_tag` is Null'
    else:
        filter_sql = ''
    con = get_con(schema_tag='core')
    tables_info = pd.read_sql(
        sql='select * from tables' + filter_sql,
        con=con
    )
    table_names = tables_info['table_name'].tolist()
    table_names_sql_filter = ', '.join([f'"{table_name}"' for table_name in table_names])
    cols_info = pd.read_sql(
        sql=f'select * from cols where `table_name` in ({table_names_sql_filter})',
        con=con
    )
    res = get_tables_from_info(tables_info=tables_info, cols_info=cols_info)
    con.close()
    return res

