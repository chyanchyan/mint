from mint.api.api_functions import *


def test_get_data_trees():
    con = get_con('data')
    res = get_empty_data_tree_json_obj_with_no_children(
        con=con,
        root='project',
    )

    print()


if __name__ == '__main__':
    test_get_data_trees()