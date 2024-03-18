import os.path

from sys_init import *
from helper_function.wrappers import sub_wrapper
from helper_function.hf_file import snapshot
from helper_function.hf_string import list_to_attr_code

from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, create_engine


@sub_wrapper(SYS_MODE)
def drop_data_schema():
    session_class = sessionmaker(bind=DB_ENGINE)
    session = session_class()

    for i, r in DB_SCHEMAS_INFO.iterrows():
        schema = f'{PROJECT_NAME}_{r["schema_tag"]}_{SYS_MODE}'
        drop_schema(session=session, schema=schema)
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
    file_path = os.path.join('meta_files/table_objs.py')
    snapshot(src_path=file_path, dst_folder=PATH_META_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def snapshot_models():
    file_path = os.path.join('meta_files/models.py')
    snapshot(src_path=file_path, dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def refresh_table_obj():
    code = open('templates/table_objs_template.py', 'r').readlines()

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
    f = open('meta_files/table_objs.py', 'w')
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

    exec('from meta_files.table_objs import get_tables')
    tables = eval('get_tables(tables_info=DB_TABLES_INFO, cols_info=DB_COLS_INFO)')
    tables_list = [v for k, v in tables.items()]
    tables_list.sort(key=lambda x: x.order)
    table_blocks = []
    for table in tables_list:
        table_blocks.append(table.to_model_code())
        table_blocks.extend(['\n', '\n'])

    res = pre_block + table_blocks + post_block

    # write to file
    f = open(f'meta_files/models.py', encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


@sub_wrapper(SYS_MODE)
def create_tables():
    exec('import meta_files.models')
    engine = create_engine(DB_URL)
    print("creating tables")
    exec('meta_files.models.Base.metadata.create_all(engine)')
    print("tables created")


@sub_wrapper(SYS_MODE)
def restore_sys():
    drop_data_schema()
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
    drop_data_schema()
    create_tables()


@sub_wrapper(SYS_MODE)
def add_table():
    refresh_table_info_to_db()
    refresh_db_info()
    snapshot_models()
    refresh_models()
    create_tables()


if __name__ == '__main__':
    restore_sys()