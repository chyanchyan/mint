from settings import *
from core.curd import CURD
from helper_function.file import mkdir
from helper_function.func import sub_wrapper


@sub_wrapper
def refresh_field_param_to_core(curd: CURD):
    dfs = pd.read_excel(os.path.join('core', 'seed.xlsx'), sheet_name=None)
    df_nodes, df_cols = dfs['nodes'], dfs['cols']
    df_nodes.to_sql(name='nodes', con=curd.con, index=False, if_exists='replace')
    curd.con.commit()
    df_cols.to_sql(name='cols', con=curd.con, index=False, if_exists='replace')
    curd.con.commit()


@sub_wrapper
def snapshot_core_db(curd: CURD):
    core_snapshot_folder = os.path.join(PATH_DB_SNAPSHOT, DB_SCHEMA_CORE)

    mkdir(core_snapshot_folder)

    if curd.table_exists(table_name='nodes'):
        curd.snapshot_table(
            table_name='nodes',
            folder=core_snapshot_folder
        )
    if curd.table_exists(table_name='cols'):
        curd.snapshot_table(
            table_name='cols',
            folder=core_snapshot_folder
        )


@sub_wrapper
def check_core_schema(curd: CURD):
    curd.create_schema(schema=DB_SCHEMA, if_exists='skip')
    curd.create_schema(schema=DB_SCHEMA_CORE, if_exists='skip')


def sys_init():
    curd_base = CURD(url=DB_URL_BASE)
    curd_core = CURD(url=DB_URL_CORE)

    check_core_schema(curd=curd_base)
    snapshot_core_db(curd=curd_core)
    refresh_field_param_to_core(curd=curd_core)


if __name__ == '__main__':
    sys_init()