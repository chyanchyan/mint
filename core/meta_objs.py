import re
from collections import OrderedDict
from typing import Dict
import json

import numpy as np

from sys_init import *
from core.curd import CURD
from helper_function.hf_string import to_json_str, dash_to_capitalized
from helper_function.hf_array import get_related_names

js_data_type_map = {
    'Integer': 'int',
    'String(.*)': 'string',
    'Date': 'date',
    'DateTime': 'date'
}


def get_nodes_info(con):
    nodes_info = pd.read_sql(sql=f'select * from {DB_SCHEMA_CORE}.nodes', con=con)
    nodes_info['order'] = nodes_info['order'].apply(lambda x: x % len(nodes_info))
    return nodes_info


def get_cols_info(con):
    cols_info = pd.read_sql(sql=f'select * from {DB_SCHEMA_CORE}.cols', con=con)
    cols_info['order'] = cols_info['order'].apply(lambda x: x % len(cols_info))
    return cols_info


def get_related_nodes_cols_info(root, nodes_info, cols_info):
    all_supers = nodes_info['super'].apply(lambda x: [item for item in x.split(',')] if x else [])
    related_nodes = get_related_names(root=root, relations=dict(zip(
        nodes_info['name'],
        all_supers
    )))
    related_nodes_info = nodes_info[nodes_info['name'].isin(related_nodes)]
    related_cols_info = cols_info[cols_info['node'].isin(related_nodes)]
    return related_nodes_info, related_cols_info


def combine_nodes_cols_order(root, nodes_info, cols_info):

    related_nodes_info, related_cols_info = get_related_nodes_cols_info(
        root=root, nodes_info=nodes_info, cols_info=cols_info
    )

    col_node_info = pd.merge(left=related_cols_info, right=related_nodes_info, left_on='node', right_on='name')
    gs = col_node_info.groupby(by='node')
    for g in gs:
        print(g)


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
        self.name = col_info['name']
        self.node = col_info['node']
        self.order = col_info['order']
        self.data_type = col_info['data_type']
        self.is_primary = col_info['is_primary']
        self.unique = col_info['unique']
        self.nullable = col_info['nullable']
        self.server_default = col_info['server_default']
        self.default = col_info['default']
        self.foreign_key = col_info['foreign_key']
        self.onupdate = col_info['onupdate']
        self.ondelete = col_info['ondelete']
        self.comment = col_info['comment']
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
                'ondelete=\'%s\'' % self.ondelete,
                'onupdate=\'%s\'' % self.onupdate
            ])
            s_fk = 'ForeignKey(%s)' % s_fk
        else:
            s_fk = None

        if not pd.isna(self.is_primary):
            s_pk = 'primary_key=%s' % ['False', 'True'][int(self.is_primary)]
        else:
            s_pk = False

        if not pd.isna(self.unique):
            s_unique = 'unique=%s' % ['False', 'True'][int(self.unique)]
        else:
            s_unique = False

        if not pd.isna(self.nullable):
            s_nullable = 'nullable=%s' % ['False', 'True'][int(self.nullable)]
        else:
            s_nullable = False

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

        s = '    %s = Column(%s)' % (self.name, s_param)

        return s

    def to_col_info(self):
        params = [param for param in self.__dir__()[: list(self.__dir__()).index('__module__')]]
        col_info = {}
        for param in params:
            col_info[param] = eval(f'self.{param}')

        return col_info


class MetaTable(JsonObj):
    def __init__(self, name, nodes_info: pd.DataFrame, cols_info: pd.DataFrame):

        super(MetaTable, self).__init__()
        if len(nodes_info) == 0 or len(cols_info) == 0:
            raise ValueError
        else:
            node_info = nodes_info[nodes_info['name'] == name].iloc[0, :]
            # node attr start
            self.id = node_info['id']
            self.name = node_info['name']
            self.super = node_info['super']
            self.label = node_info['label']
            self.order = node_info['order']
            self.type = node_info['type']
            self.comment = node_info['comment']
            # node attr end
            self._cols_info = cols_info
            self._class_name = dash_to_capitalized(self.name)
            self.cols = {}
            for col_index, col_info in cols_info.iterrows():
                self.cols[cols_info['name']] = MetaColumn(col_info=col_info)
            self.pk = [col.name for col in self.cols.values() if col.is_primary][0]
            if pd.isna(self.comment):
                self.comment = ''
            self.supers = [sp.strip() for sp in self.super.split('_')]

    def to_model_code(self):
        template_str = \
            'class %(class_name)s%(super_str)s:\n' \
            '%(class_init_block)s\n' \
            '%(node_param_block)s\n' \
            '%(col_block)s\n'

        class_init_block = \
            '    def __init__(self, *args, **kwargs):\n' \
            '        pass'

        node_param_block_template = \
            '    __tablename__ = \'%s\'\n' \
            '    __table_args__ = {\'comment\': \'%s\'}\n'

        if pd.isna(self.name):
            node_param_block = ''
            super_str = ''
        else:
            class_init_block = ''
            super_str = f"({', '.join(self.supers)})"
            node_param_block = node_param_block_template % (self.name, self.comment)

        col_block = '\n'.join([col.to_model_code() for col_name, col in self.cols.items()])

        res = template_str % {
            'class_name': self._class_name,
            'super_str': super_str,
            'class_init_block': class_init_block,
            'node_param_block': node_param_block,
            'col_block': col_block
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

    def to_nodes_info(self):
        params = [param for param in self.__dir__()[: list(self.__dir__()).index('cols_info')]]
        nodes_info = {}
        for param in params:
            nodes_info[param] = eval(f'self.{param}')

        return nodes_info


def get_table_objs(
        curd: CURD,
        node_names=(),
        nodes_info=pd.DataFrame(),
        cols_info=pd.DataFrame(),
) -> Dict[str, MetaTable]:

    con = curd.con

    res = {}

    if len(nodes_info) == 0 or len(cols_info) == 0:
        nodes_info = get_nodes_info(con=con)
        cols_info = get_cols_info(con=con)

    if len(node_names) > 0:
        tables_info = nodes_info[
            nodes_info['name'].isin(node_names)
        ]
    else:
        tables_info = nodes_info[
            nodes_info['type'] != 'class'
        ]

    for i, root_nodes_info in tables_info.iterrows():
        root_table_name = root_nodes_info['name']

        # combine class order and col order
        class_names = col_supers + [root_nodes_info['name']]
        table_class_info = class_info[class_info['name'].isin(class_names)]
        max_class_order = max([i for i in table_class_info['order'].to_list() if i])
        min_class_order = min([i for i in table_class_info['order'].to_list() if i])
        for class_i, class_row in table_class_info.iterrows():
            class_name = class_row['name']
            class_order = class_row['order']

            if pd.isna(class_order):
                class_order = max_class_order + 1
            elif class_order < 0:
                class_order = class_order + max_class_order + 1 + abs(min_class_order) + 1
            root_cols_info.loc[root_cols_info['node'] == class_name, 'order'] = \
                root_cols_info.loc[root_cols_info['node'] == class_name, 'order'].apply(
                    lambda x: x + class_order * len(root_cols_info)
                )
        res[root_nodes_info['name']] = MetaTable(
            name=root_table_name,
            nodes_info=nodes_info,
            cols_info=cols_info
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

    def get_nodes_info_detail(self):
        print(get_nodes_info(con=self.con))

    def get_cols_info_detail(self):
        print(get_cols_info(con=self.con))

    def get_table_obj(self):
        node_names = ['project', 'project_level']
        ts = get_table_objs(curd=self.curd, node_names=node_names)
        print(ts)

    def combine_nodes_cols_order(self):
        nodes_info = get_nodes_info(con=self.con)
        cols_info = get_cols_info(con=self.con)
        combine_nodes_cols_order(root='project', nodes_info=nodes_info, cols_info=cols_info)


if __name__ == '__main__':
    t = Test()
    t.combine_nodes_cols_order()