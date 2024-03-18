from helper_function.hf_data import JsonObj
from typing import List
from helper_function.hf_data import to_json_str


class AntdTableColumn(JsonObj):
    def __init__(self, title, data_index, key):
        super().__init__(self)
        self.title = title
        self.dataIndex = data_index
        self.key = key


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
        self.columns = [column.to_json_obj() for column in columns]
        self.dataSource = AntdDataSource(records=records)



def test():
    columns = [
        AntdTableColumn('type', 'type', 1),
        AntdTableColumn('value', 'value', 2),
    ]
    records = [
        {
            'type': '授信',
            'value': 3
        },
        {
            'type': 'abs',
            'value': 5
        },
        {
            'type': '非标',
            'value': 10
        },
        {
            'type': '融担',
            'value': 20
        },
        {
            'type': '其他',
            'value': 30
        },
    ]
    res = {
        'total': '3.00%',
        'table_config': AntdTableConfig(columns=columns, records=records).to_json_obj()
    }

    print(to_json_str(res))


if __name__ == '__main__':
    test()