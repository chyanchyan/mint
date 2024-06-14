import os
import pandas as pd

if 'mint' in __name__.split('.'):
    from ..api.globals import *
    from ..api.db import db_get_con, db_get_schema
    from ..meta_files.table_objs import get_tables
else:
    from mint.api.globals import *
    from mint.api.db import db_get_con, db_get_schema
    from mint.meta_files.table_objs import get_tables


def get_con(schema_tag):
    con = db_get_con(
        **DB_PARAMS,
        schema=db_get_schema(
            schema_tag=schema_tag,
            sys_mode=SYS_MODE,
            project_name=PROJECT_NAME
        )
    )
    return con


def get_table_objs():
    con = get_con(schema_tag='core')
    tables_info = pd.read_sql(
        sql='select * from tables',
        con=con
    )
    cols_info = pd.read_sql(
        sql='select * from cols',
        con=con
    )
    res = get_tables(tables_info=tables_info, cols_info=cols_info)
    con.close()
    return res

