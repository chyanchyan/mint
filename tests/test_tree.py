from mint.db.tree import *
from mint.api.base_obj import *


def test_find_child_path():
    t = Tree(
        con=get_con('data'),
        tables=get_tables('data'),
        root='project_type_ledger',
    )
    path = t.get_child_path('project_level_rate_change')
    print(path)


def test_data_tree():
    dtree = DataTree(
        con=get_con('data'),
        tables=get_tables('data'),
        root='project'
    )
    dtree.from_sql(index_col='id', index_values={'858'})

    jo = dtree.json_obj

    print(jo)


if __name__ == '__main__':
    test_data_tree()
