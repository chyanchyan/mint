from mint.db.tree import *
from mint.api.base_obj import *


def test_find_child_path():
    t = Tree(
        con=get_con('data'),
        tables=get_table_objs(),
        root='project_type_ledger',
    )
    path = t.get_child_path('project_level_rate_change')
    print(path)


if __name__ == '__main__':
    test_find_child_path()
