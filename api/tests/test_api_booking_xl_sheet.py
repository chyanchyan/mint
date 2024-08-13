from mint.api.api_booking_xl_sheet import render_booking_xl_sheet
from mint.db.tree import DataTree
from mint.sys_init import *


def test_render_booking_xl_sheet():
    con = get_con('data')
    tables = get_tables('data')
    t = DataTree(con=con, tables=tables, root='project')
    render_booking_xl_sheet(
        'test.xlsm',
        '../booking_xl_template.xlsm',
        data_tree=t
    )


if __name__ == '__main__':
    test_render_booking_xl_sheet()