import re
from collections import OrderedDict
from typing import Dict

import numpy as np

from sys_init import *
from core.curd import CURD

js_data_type_map = {
    'Integer': 'int',
    'String(.*)': 'string',
    'Date': 'date',
    'DateTime': 'date'
}


def get_nodes_info(con):
    tables_param = pd.read_sql(sql=f'select * from `{DB_SCHEMA_CORE}`.`nodes`', con=con)
    tables_param.replace({np.nan: None}, inplace=True)
    return tables_param


def get_cols_info(con):
    fields_param = pd.read_sql(sql=f'select * from `{DB_SCHEMA_CORE}`.`cols`', con=con)
    fields_param.replace({np.nan: None}, inplace=True)
    return fields_param


class JsonObj:
    def __init__(self, *args, **kwargs):
        pass

    def to_json_obj_raw(
            self,
            include_attrs=(),
            exclude_attrs=()
    ):

        res = dict()
        if len(include_attrs) == 0 or include_attrs == 'all':
            include_attrs = list(
                self.__dir__()[:list(self.__dir__()).index('__module__')]
            )

        include_attrs = sorted(
            list(set(include_attrs) - set(exclude_attrs)),
            key=lambda x: include_attrs.index(x)
        )

        for attr in include_attrs:
            value = eval('self.%s' % attr)
            if isinstance(value, (list, tuple, set)):
                try:
                    res[attr] = []
                except AttributeError:
                    continue

                for v in value:
                    if v:
                        try:
                            res[attr].append(
                                v.to_json_obj_raw(include_attrs=include_attrs)
                            )
                        except AttributeError:
                            res[attr].append(v)

            elif isinstance(value, dict):
                try:
                    res[attr] = dict()
                except AttributeError:
                    continue
                for k, v in value.items():
                    try:
                        res[attr][k] = v.to_json_obj_raw(include_attrs=include_attrs)
                    except AttributeError:
                        res[attr][k] = v

            else:
                try:
                    res[attr] = value.to_json_obj_raw(include_attrs=include_attrs)
                except AttributeError:
                    res[attr] = value

        return res

    def to_json(self, include_attrs=(), exclude_attrs=()):
        jo = self.to_json_obj_raw(
            include_attrs=include_attrs,
            exclude_attrs=exclude_attrs
        )
        return to_json_str(jo)

    def to_json_obj(self, include_attrs=(), exclude_attrs=()):
        return json.loads(
            self.to_json(include_attrs=include_attrs, exclude_attrs=exclude_attrs)
        )


class MetaColumn(JsonObj):
    def __init__(self, col_info: pd.Series):
        super(MetaColumn, self).__init__()
        # col attr start
        self.id = col_info['id']
        self.class_name = col_info['class_name']
        self.field = col_info['field']
        self.order = col_info['order']
        self.data_type = col_info['data_type']
        self.is_primary = col_info['is_primary']
        self.is_row_web_label = col_info['is_row_web_label']
        self.foreign_key = col_info['foreign_key']
        self.unique = col_info['unique']
        self.on_delete = col_info['on_delete']
        self.on_update = col_info['on_update']
        self.nullable = col_info['nullable']
        self.autoincrement = col_info['autoincrement']
        self.default = col_info['default']
        self.server_default = col_info['server_default']
        self.onupdate = col_info['onupdate']
        self.comment = col_info['comment']
        self.web_obj = col_info['web_obj']
        self.web_visible = col_info['web_visible']
        self.web_activate = col_info['web_activate']
        self.web_label = col_info['web_label']
        self.web_detail_format = col_info['web_detail_format']
        self.web_template_api_format = col_info['web_template_api_format']
        self.web_fill_instructions = col_info['web_fill_instructions']
        self.web_list_order = col_info['web_list_order']
        self.js_data_type = col_info['js_data_type']
        # col attr end
        
        if isinstance(self.default, str):
            self.default_value_data_type = 'str'
            if self.default[0] == self.default[-1] == '"':
                self.default = self.default.strip('"')
        else:
            self.default_value_data_type = 'non_str'

        for key, value in js_data_type_map.items():
            if re.match(pattern=key, string=self.data_type):
                self.js_data_type = value
                break
        else:
            self.js_data_type = None

    def to_model_code(self):
        if not pd.isna(self.foreign_key):
            s_fk = ', '.join([
                '\'%s\'' % self.foreign_key,
                'ondelete=\'%s\'' % self.on_delete,
                'onupdate=\'%s\'' % self.on_update
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
                s_default = f'default="{self.default}"'
            else:
                s_default = f'default={self.default}'
        else:
            s_default = None

        if not pd.isna(self.server_default):
            s_server_default = f'server_default="{str(self.server_default)}"'
        else:
            s_server_default = None

        if not pd.isna(self.onupdate):
            s_onupdate = 'onupdate=%s' % self.onupdate
        else:
            s_onupdate = None

        if not pd.isna(self.comment):
            s_comment = 'comment=\'%s\'' % self.comment
        else:
            s_comment = None

        params = [self.data_type, s_fk, s_pk, s_nullable, s_unique, s_default,
                  s_server_default, s_onupdate, s_comment]

        s_param = ', '.join([item for item in params if item])

        s = '    %s = Column(%s)' % (self.field, s_param)

        return s

    def to_field_info(self):
        params = [param for param in self.__dir__()[: list(self.__dir__()).index('__module__')]]
        field_info = {}
        for param in params:
            field_info[param] = eval(f'self.{param}')

        return field_info


class MetaTable(JsonObj):
    def __init__(self, table_info: pd.Series = (), fields_info: pd.DataFrame = ()):
        super(MetaTable, self).__init__()
        if len(table_info) == 0 or len(fields_info) == 0:
            self.class_name = None
            self.table_name = None
            self.comment = None
            self.cols = {}
        else:
            # table attr start
            self.index = table_info['id']
            self.class_name = table_info['class_name']
            self.table_name = table_info['table_name']
            self.comment = table_info['comment']
            self.order = table_info['order']
            self.ancestors = table_info['ancestors']
            self.analysis_ancestors = table_info['analysis_ancestors']
            self.web_list_index = table_info['web_list_index']
            self.web_booking_children = table_info['web_booking_children']
            # table attr end
            self.fields_info = fields_info
            self.cols = {}
            for col_index, col_info in fields_info.iterrows():
                self.cols[col_info['field']] = MetaColumn(col_info=col_info)
            self.pk = [col.field for col in self.cols.values() if col.is_primary][0]
            if pd.isna(self.comment):
                self.comment = ''

    def to_model_code(self):
        template_str = \
            'class %(class_name)s%(ancestors_str)s:\n' \
            '%(class_init_block)s\n' \
            '%(table_param_block)s\n' \
            '%(column_block)s\n'

        ancestors_str_template = \
            '(%s)'

        class_init_block = \
            '    def __init__(self, *args, **kwargs):\n' \
            '        pass'

        table_param_block_template = \
            '    __tablename__ = \'%s\'\n' \
            '    __table_args__ = {\'comment\': \'%s\'}\n'

        if pd.isna(self.table_name):
            table_param_block = ''
            ancestors_str = ''
        else:
            class_init_block = ''
            ancestors_str = ancestors_str_template % self.ancestors
            table_param_block = table_param_block_template % (self.table_name, self.comment)

        fields = self.fields_info['field'].to_list()
        orders = self.fields_info['order'].to_list()
        orders = [order % len(orders) for order in orders]
        fields = np.array(sorted(zip(orders, fields)))[:, 1]

        column_block = '\n'.join([self.cols[col_name].to_model_code()
                                  for col_name in fields])

        res = template_str % {
            'class_name': self.class_name,
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
        params = [param for param in self.__dir__()[: list(self.__dir__()).index('fields_info')]]
        table_info = {}
        for param in params:
            table_info[param] = eval(f'self.{param}')

        return table_info


def get_table_objs(
        con,
        table_names=(),
        classes_tables_info=pd.DataFrame(),
        fields_info=pd.DataFrame(),
) -> Dict[str, MetaTable]:

    res = {}

    if len(classes_tables_info) == 0 or len(fields_info) == 0:
        classes_tables_info = get_nodes_info(con=con)
        fields_info = get_cols_info(con=con)

    if len(table_names) > 0:
        tables_info = classes_tables_info[
            classes_tables_info['table_name'].isin(table_names)
        ]
    else:
        tables_info = classes_tables_info[
            ~pd.isna(classes_tables_info['table_name'])
        ]

    for i, root_table_info in tables_info.iterrows():
        root_table_name = root_table_info['table_name']

        class_info = classes_tables_info[
            (pd.isna(classes_tables_info['table_name'])) |
            (classes_tables_info['table_name'] == root_table_name)
        ]

        if not pd.isna(root_table_info['ancestors']):
            field_ancestors = [a.strip() for a in root_table_info['ancestors'].split(',')]
        else:
            field_ancestors = []

        root_fields_info = fields_info[
            (fields_info['class_name'] == root_table_info['class_name']) |
            fields_info['class_name'].isin(field_ancestors)
            ]

        # combine class order and col order
        class_names = field_ancestors + [root_table_info['class_name']]
        table_class_info = class_info[class_info['class_name'].isin(class_names)]
        max_class_order = max([i for i in table_class_info['order'].to_list() if i])
        min_class_order = min([i for i in table_class_info['order'].to_list() if i])
        for class_i, class_row in table_class_info.iterrows():
            class_name = class_row['class_name']
            class_order = class_row['order']

            if pd.isna(class_order):
                class_order = max_class_order + 1
            elif class_order < 0:
                class_order = class_order + max_class_order + 1 + abs(min_class_order) + 1
            root_fields_info.loc[
                root_fields_info['class_name'] == class_name, 'order'
            ] = root_fields_info.loc[
                root_fields_info['class_name'] == class_name, 'order'
            ].apply(
                lambda x: x + class_order * len(root_fields_info)
            )
        res[root_table_info['table_name']] = MetaTable(
            table_info=root_table_info,
            fields_info=root_fields_info
        )

    return res


class Test:
    curd = CURD(url=DB_URL)
    con = curd.engine.connect()
    @staticmethod
    def ordered_dict():
        d = OrderedDict(
            [('a', 3), ('b', 2), ('c', 1)]
        )
        print(d)
        print(d.keys())
        print(d.values())
        print(d)

    def get_table_info_detail(self):
        print(get_nodes_info(con=self.con))

    def get_field_info_detail(self):
        print(get_cols_info(con=self.con))

    def get_table_obj(self):
        table_names = ['project', 'project_level']
        ts = get_table_objs(con=self.con, table_names=table_names)
        print(ts)


if __name__ == '__main__':
    t = Test()
    # t.get_table_info_detail()
    # t.get_field_info_detail()
    t.get_table_obj()

