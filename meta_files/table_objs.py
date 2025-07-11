"""
当前文件为table_objs.py的模板，每次会根据table_info.xlsx和meta_file下的这个模板
生成meta文件夹下的table_objs.py文件
"""

import re
import sys
import os
import pandas as pd
import numpy as np
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)


from mint.helper_function.hf_number import is_number
from mint.helper_function.hf_data import JsonObj
from mint.helper_function.hf_string import dash_name_to_camel


js_data_type_map = {
    'Integer': 'int',
    'String(.*)': 'string',
    'Date': 'date',
    'DateTime': 'date'
}


class MetaColumn(JsonObj):
    def __init__(self, table_info: pd.Series, col_info: pd.Series, order):
        super(MetaColumn, self).__init__()
        # 下面这行是个定位注释，勿删改
        # col attr start
        self.table_name = col_info['table_name']
        self.col_name = col_info['col_name']
        self.label = col_info['label']
        self.fill_instructions = col_info['fill_instructions']
        self.data_type = col_info['data_type']
        self.is_primary = col_info['is_primary']
        self.check_pk = col_info['check_pk']
        self.is_index = col_info['is_index']
        self.unique = col_info['unique']
        self.foreign_key = col_info['foreign_key']
        self.fk_on_delete = col_info['fk_on_delete']
        self.fk_on_update = col_info['fk_on_update']
        self.on_update = col_info['on_update']
        self.nullable = col_info['nullable']
        self.autoincrement = col_info['autoincrement']
        self.default = col_info['default']
        self.server_default = col_info['server_default']
        self.naming_field_order = col_info['naming_field_order']
        self.web_obj = col_info['web_obj']
        self.web_visible = col_info['web_visible']
        self.web_activate = col_info['web_activate']
        self.is_row_web_label = col_info['is_row_web_label']
        self.web_detail_format = col_info['web_detail_format']
        self.web_template_api_format = col_info['web_template_api_format']
        self.web_list_order = col_info['web_list_order']
        self.comment = col_info['comment']
        # col attr end
        # 上面这行是个定位注释，勿删改

        self.order = order
        self.table_info = table_info
        self.col_info = col_info

        if is_number(self.default):
            self.default_value_data_type = 'non_str'
            self.default = float(self.default)
        else:
            if not pd.isna(self.default):
                if self.default[-2:] == '()':
                    self.default_value_data_type = 'func'
                else:
                    self.default_value_data_type = 'str'
                    if str(self.default)[0] == str(self.default)[-1] == '"':
                        self.default = str(self.default).strip('"')

        for key, value in js_data_type_map.items():
            if re.match(pattern=key, string=self.data_type):
                self.js_data_type = value
                break
        else:
            self.js_data_type = None

    def to_model_code(self):
        if not pd.isna(self.foreign_key):
            fk_schema_tag, fk_table, fk_col = self.foreign_key.split('.')
            fk_schema = DB_SCHEMAS[fk_schema_tag]
            s_fk = ', '.join([
                'f\'%s.%s.%s\'' % (fk_schema, fk_table, fk_col),
                'ondelete=\'%s\'' % self.fk_on_delete,
                'onupdate=\'%s\'' % self.fk_on_update
            ])
            s_fk = 'ForeignKey(%s)' % s_fk
        else:
            s_fk = None

        if not pd.isna(self.is_primary):
            s_pk = 'primary_key=%s' % ['False', 'True'][int(self.is_primary)]
        else:
            s_pk = None

        if not pd.isna(self.unique):
            s_unique = 'unique=%s' % ['False', 'True'][int(self.unique)]
        else:
            s_unique = None

        if not pd.isna(self.nullable):
            s_nullable = 'nullable=%s' % ['False', 'True'][int(self.nullable)]
        else:
            s_nullable = None

        if not pd.isna(self.default):
            if self.default_value_data_type == "str":
                s_default = f"default='{self.default}'"
            else:
                s_default = f'default={self.default}'
        else:
            s_default = None

        if not pd.isna(self.server_default):
            if isinstance(self.server_default, str) and self.server_default[-2:] == '()':
                s_server_default = f'server_default={str(self.server_default)}'
            else:
                s_server_default = f"server_default='{str(self.server_default)}'"
        else:
            s_server_default = None

        if not pd.isna(self.autoincrement):
            s_autoincrement = 'autoincrement=%s' % ['False', 'True'][int(self.autoincrement)]
        else:
            s_autoincrement = None

        if not pd.isna(self.on_update):
            if self.on_update[-2:] == '()':
                s_onupdate = "onupdate=%s" % str(self.on_update)
            else:
                s_onupdate = "onupdate='%s'" % str(self.on_update)
        else:
            s_onupdate = None

        if not pd.isna(self.comment):
            s_comment = 'comment=\'%s\'' % self.comment
        else:
            s_comment = None

        params = [self.data_type, s_fk, s_pk, s_nullable, s_unique, s_default,
                  s_server_default, s_autoincrement, s_onupdate, s_comment]

        s_param = ', '.join([item for item in params if item])

        s = '    %s = Column(%s)' % (self.col_name, s_param)

        return s

    def to_col_info(self):
        params = [param for param in self.__dir__()[: list(self.__dir__()).index('__module__')]]
        col_info = {}
        for param in params:
            col_info[param] = eval(f'self.{param}')

        return col_info


class MetaTable(JsonObj):
    def __init__(
            self,
            table_info: pd.Series = (),
            cols_info: pd.DataFrame = (),
            order=None
    ):
        super(MetaTable, self).__init__()
        if len(table_info) == 0 or len(cols_info) == 0:
            self.class_name = None
            self.table_name = None
            self.comment = None
            self.cols = {}
        else:
            # table attr start
            self.table_name = table_info['table_name']
            self.label = table_info['label']
            self.schema_tag = table_info['schema_tag']
            self.ancestors = table_info['ancestors']
            self.auto_name_param = table_info['auto_name_param']
            self.web_list_index = table_info['web_list_index']
            self.comment = table_info['comment']
            # table attr end
            if pd.isna(self.schema_tag):
                self.schema = None
            else:
                self.schema = DB_SCHEMAS[self.schema_tag]
            self.cols_info = cols_info
            self.cols = {
                col_info['col_name']: MetaColumn(table_info=table_info, col_info=col_info, order=i)
                for i, (col_index, col_info) in enumerate(cols_info.iterrows())
            }

            self.pk = [col.col_name for col in self.cols.values() if col.is_primary][0]
            if pd.isna(self.comment):
                self.comment = ''
        self.order = order

    def to_model_code(self):
        template_str = \
            'class %(class_name)s%(ancestors_str)s:\n' \
            '%(class_init_block)s\n' \
            '%(table_param_block)s\n' \
            '%(column_block)s\n'

        ancestors_str_template = \
            '(%s)'

        table_param_block_template = \
            '    __tablename__ = \'%s\'\n' \
            '    __table_args__ = {\'schema\': \'%s\', \'comment\': \'%s\'}\n'

        if pd.isna(self.ancestors):
            class_init_block = \
                '    def __init__(self, *args, **kwargs):\n' \
                '        pass'
            ancestors_str = ''
            table_param_block = ''
        else:
            class_init_block = ''
            ancestors_list = [
                dash_name_to_camel(a.strip())
                for a in self.ancestors.split(',')
            ]

            if self.schema is None:
                table_param_block = ''
            else:
                ancestors_list = ['Base'] + ancestors_list
                table_param_block = table_param_block_template % (self.table_name, self.schema, self.comment)

            ancestors = ', '.join(ancestors_list)
            ancestors_str = ancestors_str_template % ancestors
        col_list = [col for k, col in self.cols.items()]
        col_list.sort(key=lambda x: x.order)

        column_block = '\n'.join([col.to_model_code() for col in col_list])

        res = template_str % {
            'class_name': dash_name_to_camel(self.table_name),
            'ancestors_str': ancestors_str,
            'class_init_block': class_init_block,
            'table_param_block': table_param_block,
            'column_block': column_block
        }

        return res

    def to_json_obj(self, **kwargs):
        try:
            exclude_attrs = list(kwargs['exclude_attrs'])
        except KeyError:
            exclude_attrs = []
        res = super().to_json_obj(exclude_attrs=['cols'] + exclude_attrs)
        res['cols'] = {col_name: col.to_json_obj() for col_name, col in self.cols.items()}
        return res

    def to_table_info(self):
        params = [param for param in self.__dir__()[: list(self.__dir__()).index('cols_info')]]
        table_info = {}
        for param in params:
            table_info[param] = eval(f'self.{param}')

        return table_info


def get_tables(tables_info, cols_info):
    res = {}
    table_names = tables_info[
        ~pd.isna(tables_info['schema_tag'])
    ]['table_name'].tolist()

    for i, table_r in tables_info.iterrows():
        table_name = table_r['table_name']
        if table_name not in table_names and not pd.isna(table_r['schema_tag']):
            continue

        if not pd.isna(table_r['ancestors']):
            ancestors = [a.strip() for a in table_r['ancestors'].split(',')]
        else:
            ancestors = []

        ancestors_copy = list(ancestors)
        base_ancestor_tables = []
        while len(ancestors_copy) > 0:
            ancestor = ancestors_copy.pop()
            ancestor_row = tables_info[tables_info['table_name'] == ancestor]

            if not pd.isna(ancestor_row['ancestors'].values[0]):
                ancestor_ancestors = [
                    a.strip()
                    for a in ancestor_row['ancestors'].values[0].split(',')
                ]
                ancestors_copy.extend(ancestor_ancestors)

            base_ancestor_tables.append(ancestor)

        table_cols_info = cols_info[
            (cols_info['table_name'] == table_name) |
            cols_info['table_name'].isin(base_ancestor_tables)
            ]

        res[table_r['table_name']] = MetaTable(
            table_info=table_r,
            cols_info=table_cols_info,
            order=i
        )

    return res