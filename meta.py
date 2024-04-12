import os.path
from datetime import datetime as dt
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, create_engine

if 'mint' in __name__.split('.'):
    from .sys_init import *
    from .helper_function.wrappers import sub_wrapper
    from .helper_function.hf_file import snapshot, mkdir
    from .helper_function.hf_string import list_to_attr_code
    from .helper_function.hf_db import export_xl
else:
    from sys_init import *
    from helper_function.wrappers import sub_wrapper
    from helper_function.hf_file import snapshot, mkdir
    from helper_function.hf_string import list_to_attr_code
    from helper_function.hf_db import export_xl


@sub_wrapper(SYS_MODE)
def drop_schemas(schema_tags=None):
    if schema_tags is None:
        schema_tags = []
        for i, r in DB_SCHEMAS_INFO.iterrows():
            schema_tags.append(r['schema_tag'])

    session_class = sessionmaker(bind=DB_ENGINE)
    session = session_class()

    for schema_tag in schema_tags:
        drop_schema(session=session, schema=DB_SCHEMAS[schema_tag])
    session.commit()
    session.close()


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
    file_path = os.path.join(PATH_ROOT, 'meta_files/table_objs.py')
    snapshot(src_path=file_path, dst_folder=PATH_META_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def snapshot_models():
    file_path = os.path.join(PATH_ROOT, 'meta_files/models.py')
    snapshot(src_path=file_path, dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def refresh_table_obj():
    template_pth = os.path.join(PATH_ROOT, 'templates/table_objs_template.py')
    output_pth = os.path.join(PATH_ROOT, 'meta_files/table_objs.py')
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
    template_pth = os.path.join(PATH_ROOT, 'templates/models_template.py')
    output_pth = os.path.join(PATH_ROOT, 'meta_files/models.py')
    f = open(template_pth, encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    if 'mint' in __name__.split('.'):
        exec('from .meta_files.table_objs import get_tables')
    else:
        exec('from mint.table_objs import get_tables')
    tables = eval('get_tables(tables_info=DB_TABLES_INFO, cols_info=DB_COLS_INFO)')
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


def snapshot_database(comments=''):
    folder = dt.now().strftime('%Y%m%d_%H%M%S_%f') + f'-{comments}'
    if not os.path.exists(PATH_DB_SNAPSHOT):
        mkdir(PATH_DB_SNAPSHOT)

    for schema in DB_SCHEMAS.values():
        folder = os.path.join(
            PATH_DB_SNAPSHOT,
            folder,
            schema
        )
        if not os.path.exists(folder):
            mkdir(folder)

        export_xl(
            output_folder=folder,
            con=DB_ENGINE,
            schema=schema,
            table_names=None
        )


@sub_wrapper(SYS_MODE)
def create_tables():
    if 'mint' in __name__.split('.'):
        exec('from .meta_files import models')
    else:
        exec('from mint.meta_files import models')
    engine = create_engine(DB_URL)
    print("creating tables")
    exec('models.Base.metadata.create_all(engine)')
    print("tables created")


@sub_wrapper(SYS_MODE)
def restore_sys():
    drop_schemas()
    refresh_table_info_to_db()
    refresh_db_info()
    snapshot_table_obj()
    refresh_table_obj()
    snapshot_models()
    refresh_models()
    create_tables()


@sub_wrapper(SYS_MODE)
def restore_table():
    refresh_table_info_to_db()
    refresh_db_info()
    snapshot_table_obj()
    refresh_table_obj()


@sub_wrapper(SYS_MODE)
def restore_db():
    drop_schemas(['data'])
    create_tables()


@sub_wrapper(SYS_MODE)
def add_table():
    refresh_table_info_to_db()
    refresh_db_info()
    snapshot_models()
    refresh_models()
    create_tables()


def init_sys():
    refresh_table_info_to_db()
    refresh_models()
    refresh_table_obj()
