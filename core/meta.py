import re
from datetime import datetime as dt

from sys_init import *
from helper_function.file import snapshot
from helper_function.func import sub_wrapper
from curd import CURD
from core.meta_objs import MetaTable

FIELD_PARAM_FILE_NAME = 'field_param.xlsx'


def load_nodes_cols(curd: CURD):
    class_sql = f'select * from {DB_SCHEMA_CORE}.nodes'
    field_sql = f'select * from {DB_SCHEMA_CORE}.cols'
    class_info = pd.read_sql(sql=class_sql, con=curd.con)
    field_info = pd.read_sql(sql=field_sql, con=curd.con)
    return class_info, field_info


@sub_wrapper
def snapshot_models():
    model_file_path = os.path.join('core', 'models.py')
    snapshot(src_path=model_file_path, dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper
def snapshot_meta():
    meta_file_path = os.path.join('core', 'meta_objs.py')
    snapshot(src_path=meta_file_path, dst_folder=PATH_META_SNAPSHOT)


@sub_wrapper
def refresh_models(curd: CURD):

    # backup models file
    old_models_file_path = os.path.join('core', 'models.py')

    # load models file template
    f = open(old_models_file_path, encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    # load fields param
    class_info, fields_info = load_nodes_cols(curd=curd)

    table_blocks = []
    for table_index, table_info_row in class_info.iterrows():
        if pd.isna(table_info_row['ancestors']):
            field_ancestors = []
        else:
            field_ancestors = [a.strip() for a in table_info_row['ancestors'].split(',')]
        table_fields_info = fields_info[
            (fields_info['class_name'] == table_info_row['class_name']) |
            fields_info['class_name'].isin(field_ancestors)
        ]
        t = MetaTable(table_info=table_info_row, fields_info=table_fields_info)
        table_blocks.append(t.to_model_code())
        table_blocks.extend(['\n', '\n'])

    res = pre_block + table_blocks + post_block

    # write to file
    f = open(old_models_file_path, encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


def refresh_column_class(code, explicit=False):
    attr_code_str_base = " " * 8 + "self.%s = col_info['%s']\n"
    st_comment_str = ' ' * 8 + '# col attr start\n'
    ed_comment_str = ' ' * 8 + '# col attr end\n'

    field_attrs = load_nodes_cols()[1].columns
    st_index = code.index(st_comment_str)
    ed_index = code.index(ed_comment_str)

    if explicit:
        insert_code = [attr_code_str_base % (attr, attr) for attr in field_attrs]
    else:
        insert_code = [
            '        for key in col_info.columns:\n',
            '            exec("self.%s = col_info[\'%s\']" % (key, key))\n'
        ]
    res_code = code[: st_index + 1] + insert_code + code[ed_index:]
    return res_code


def refresh_table_class(code, explicit=False):
    attr_code_str_base = " " * 12 + "self.%s = table_info['%s']\n"
    st_comment_str = ' ' * 12 + '# table attr start\n'
    ed_comment_str = ' ' * 12 + '# table attr end\n'

    table_attrs = load_nodes_cols()[0].columns
    st_index = code.index(st_comment_str)
    ed_index = code.index(ed_comment_str)

    if explicit:
        insert_code = [attr_code_str_base % (attr, attr) for attr in table_attrs]
    else:
        insert_code = [
            '        for key in table_info.columns:\n',
            '            exec("self.%s = table_info[\'%s\']" % (key, key))\n'
        ]
    res_code = code[: st_index + 1] + insert_code + code[ed_index:]
    return res_code


@sub_wrapper
def refresh_meta(explicit=True):
    code = open(os.path.join('core', 'meta_objs.py'), 'r').readlines()
    code = refresh_column_class(code=code, explicit=explicit)
    code = refresh_table_class(code=code, explicit=explicit)
    f = open(os.path.join('', 'meta_objs.py'), 'w')
    f.writelines(code)
    f.close()


#@confirm_wrapper
@sub_wrapper
def restore_sys():
    snapshot_models()
    snapshot_meta()

    refresh_field_param_to_core()
    refresh_meta()
    refresh_models()

    create_tables()


def refresh_sys():

    db_bak_folder = os.path.join('', 'db_snapshots')

    refresh_field_param_to_core(path=os.path.join('', 'field_param.xlsm'))

    restore_sys()

    last_sn = get_last_snapshot_timestamp(db_bak_folder)
    db_bak_folder = os.path.join(db_bak_folder, last_sn)
    table_names = get_booking_sequence()

    migrate_from_excel(xl_folder=db_bak_folder, table_names=table_names)


def models_code_list_to_excel(model_code_list: list):
    table_name_row_indexes = [index for index in range(len(model_code_list))
                              if '__tablename__' in model_code_list[index]
                              ]
    table_code_block_indexes = zip(
        table_name_row_indexes,
        table_name_row_indexes[1:] + [len(table_name_row_indexes)]
    )
    data = pd.DataFrame()
    for table_st_index, table_ed_index in table_code_block_indexes:
        table_code_block = model_code_list[table_st_index: table_ed_index]
        table_name_row = model_code_list[table_st_index]
        table_name = table_name_row.split(' = ')[1].strip().strip("'")
        col_rows = [row.strip() for row in table_code_block
                    if row.strip()[:2] != '__' and ' = ' in row]
        col_names = [row.split(' = ')[0].strip("'") for row in col_rows]
        col_types = [re.findall('Column\\((.*)(.*),', row)[0][0].split(',')[0]
                     for row in col_rows]
        res = pd.DataFrame(data=np.array([col_names, col_types]).transpose())
        res['table'] = table_name
        data = data.append(res, ignore_index=True)

    return data


def models_to_excel():
    rs = open(os.path.join('..', '_core_bak', 'models_bak.py'), encoding='utf-8').readlines()
    data = models_code_list_to_excel(rs)
    data.to_excel('model_output.xlsx')


def refresh_tables_by_new_field_params():
    snapshot_field_params()
    refresh_field_param_to_core()
    snapshot_models()
    refresh_models()
    snapshot_database()
    create_tables()


def update_field_params_file(field_params_file_path, tables_info, fields_info):
    df = pd.read_excel(field_params_file_path, sheet_name=None)
    df['class'] = tables_info
    df['field'] = fields_info

    writer = ExcelWriter(field_params_file_path, mode='w', engine='openpyxl', options={'strings_to_urls': False})

    for table_name, table in df.items():
        table.to_excel(excel_writer=writer, sheet_name=table_name, index=False)

    writer.save()


def add_table(table: MetaTable):
    # snapshot_field_params()
    # snapshot_models()
    # backup_field_params_file()

    db_tables_info = get_tables_info_detail()
    db_fields_info = get_fields_info_detail()

    table_info = [table.to_table_info()]
    fields_info = [col.to_field_info() for i, col in table.cols.items() if
                   col.class_name not in table.ancestors.split(', ')]

    tables_info = pd.concat([db_tables_info, pd.DataFrame(table_info)], ignore_index=True)
    fields_info = pd.concat([db_fields_info, pd.DataFrame(fields_info)], ignore_index=True)

    print(tables_info)
    print(fields_info)

    update_field_params_file(FIELD_PARAM_FILE_NAME, tables_info, fields_info)
    #
    # refresh_field_param_to_core()
    # refresh_models()
    # create_tables()


def test_update_field_params_file():
    table_info = pd.DataFrame(
        [
            {
                'index': 1,
                'class_name': 'TestAddTable',
                'table_name': 'test_add_table',
                'comment': 'test',
                'ancestors': 'Base, Id, TimeStamped'
            }
        ]
    )
    fields_info = pd.DataFrame(
        [
            {
                'class_name': 'TestAddTable',
                'field': 'col1',
                'order': 0,
                'comment': 'col1_comment',
                'nullable': 1,
                'web_label': 'col1_web_label'
            }
        ],
    )

    update_field_params_file('../../core/field_param_v3.xlsx', table_info, fields_info)


def test_add_table():

    table = get_table_objs('project')['project']
    table.table_name = 'project_2'
    table.class_name = 'Project2'
    cols = table.cols
    for col_name, col in table.cols.items():
        cols[col_name].class_name = 'Project2'
    table.cols = cols
    add_table(table=table)


if __name__ == '__main__':
    print(load_nodes_cols(curd=CURD(DB_URL_CORE)))