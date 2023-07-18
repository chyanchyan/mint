import re
from datetime import datetime as dt

from sys_init import *
from helper_function.file import snapshot
from helper_function.func import sub_wrapper
from curd import CURD
from core.meta_objs import MetaTable


def load_nodes_cols(curd: CURD):
    nodes_sql = f'select * from {DB_SCHEMA_CORE}.nodes'
    cols_sql = f'select * from {DB_SCHEMA_CORE}.cols'
    nodes_info = pd.read_sql(sql=nodes_sql, con=curd.con)
    cols_info = pd.read_sql(sql=cols_sql, con=curd.con)
    return nodes_info, cols_info


def refresh_code_by_df(code, df, df_var_name, st_mark, ed_mark, intent_blocks):
    attr_code_str_base = " " * 4 * intent_blocks + f"self.%s = {df_var_name}['%s']\n"
    st_index = code.index(st_mark)
    ed_index = code.index(ed_mark)
    insert_code = [attr_code_str_base % (attr, attr) for attr in df.columns]
    res_code = code[: st_index + 1] + insert_code + code[ed_index:]
    return res_code


@sub_wrapper
def snapshot_models():
    model_file_path = os.path.join(PATH_ROOT, 'core', 'models.py')
    snapshot(src_path=model_file_path, dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper
def snapshot_meta_obj():
    meta_file_path = os.path.join(PATH_ROOT, 'core', 'meta_objs.py')
    snapshot(src_path=meta_file_path, dst_folder=PATH_META_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper
def refresh_meta_obj(curd: CURD):
    snapshot_meta_obj()
    nodes_info, cols_info = load_nodes_cols(curd=curd)
    code = open('meta_objs.py', 'r').readlines()

    st_mark = ' ' * 8 + '# col attr start\n'
    ed_mark = ' ' * 8 + '# col attr end\n'
    code = refresh_code_by_df(code=code, df=cols_info, df_var_name='col_info', st_mark=st_mark, ed_mark=ed_mark,
                              intent_blocks=2)
    st_mark = ' ' * 12 + '# node attr start\n'
    ed_mark = ' ' * 12 + '# node attr end\n'
    code = refresh_code_by_df(code=code, df=nodes_info, df_var_name='node_info', st_mark=st_mark, ed_mark=ed_mark,
                              intent_blocks=3)
    f = open('meta_objs.py', 'w')
    f.writelines(code)
    f.close()


@sub_wrapper
def refresh_models(curd: CURD):

    # backup models file
    old_models_file_path = os.path.join(PATH_ROOT, 'core', 'models.py')

    # load models file template
    f = open(old_models_file_path, encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    # load fields param
    nodes_info, cols_info = load_nodes_cols(curd=curd)

    table_blocks = []
    for table_index, table_info_row in nodes_info.iterrows():
        if pd.isna(table_info_row['super']):
            col_supers = []
        else:
            col_supers = [a.strip() for a in table_info_row['super'].split(',')]
        table_fields_info = cols_info[
            (cols_info['node'] == table_info_row['name']) |
            cols_info['node'].isin(col_supers)
        ]
        t = MetaTable(nodes_info=table_info_row, cols_info=table_fields_info)
        table_blocks.append(t.to_model_code())
        table_blocks.extend(['\n', '\n'])

    res = pre_block + table_blocks + post_block

    # write to file
    f = open(old_models_file_path, encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


if __name__ == '__main__':
    snapshot_meta_obj()
