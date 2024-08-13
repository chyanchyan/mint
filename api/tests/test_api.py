from mint.api.api import *


def test_get_data_tree():
    table_name = 'project_level_rate_change'
    res = get_empty_data_tree_json_obj_with_no_children(root=table_name)
    res_str = to_json_str(res)
    res_obj = to_json_obj(res_str)
    print(res_str)


if __name__ == '__main__':
    test_get_data_tree()