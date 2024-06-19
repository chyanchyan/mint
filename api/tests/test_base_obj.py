from mint.api.base_obj import *


def test_get_table_objs():
    table_objs = get_table_objs(schema_tag='data')
    print(table_objs)


if __name__ == '__main__':
    test_get_table_objs()