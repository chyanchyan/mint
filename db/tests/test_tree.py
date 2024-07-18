from mint.settings import *
from mint.db.tree import *
from mint.sys_init import *
from mint.api.base_obj import get_tables

con = get_con('data')
tables = get_tables('data')


def test_get_cst_pki():
    res = get_cst_pki(con=con, schemas=[get_schema('data')])
    print(res)


def test_get_booking_sequence():
    res = get_booking_sequence(
        cst_pki=get_cst_pki(con=con, schemas=[get_schema('data')]))


def test_tree():
    datat = DataTree(
        con=con,
        tables=tables,
        root='project'
    )
    datat.from_sql(
        index_col='name',
        index_values={'上海授信-20210628'}
    )
    jo = datat.json_obj
    print()


if __name__ == '__main__':
    test_tree()