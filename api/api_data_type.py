import sys
import os
from typing import List

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from mint.helper_function.hf_data import JsonObj
from mint.helper_function.hf_string import to_json_str


class AntdTableColumn(dict):
    def __init__(self, col_obj, key):
        items = {
            'key': key,
            'dataIndex': col_obj.col_name,
            'title': col_obj.label,
            'dataType': col_obj.data_type
        }
        super().__init__(items)


class AntdTableColumns(list):
    def __init__(self, col_objs):
        super().__init__(
            [
                AntdTableColumn(
                    col_obj=col_obj,
                    key=key
                ) for key, col_obj in enumerate(col_objs)
            ]
        )


class AntdDataSource(list):
    def __init__(self, records):
        res = []
        for i, item in enumerate(records, start=1):
            res.append(
                {
                    'key': str(i),
                    **item
                }
            )
        super().__init__(res)


class AntdTableConfig(JsonObj):
    def __init__(self, columns: List[AntdTableColumn], records: list):
        super().__init__(self)
        self.columns = columns
        self.dataSource = AntdDataSource(records=records)