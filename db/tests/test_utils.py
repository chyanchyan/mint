import pandas as pd

from mint.db.utils import db_refresh_delta_by_to
from algo.data import trim_and_mark_fglbf
from mint.sys_init import get_con
from mint.settings import *


def test_db_refresh_delta_by_to():
    con = get_con('data')
    data = pd.read_sql(
        'select * from financing_guarantee_loan_balance_flow '
        # 'where unionloan_institution_name = "昆仑银行股份有限公司"'
        ,
    con=con
    ).add_prefix('fglbf.')
    i = pd.read_sql(
        'select * from inst',
        con=con
    ).add_prefix('i.')

    data = trim_and_mark_fglbf(data, i)
    data = db_refresh_delta_by_to(
        df_to_fill=data,
        index='fglbf.project_level_name',
        date_col='fglbf.change_date',
        to_col='fglbf.rate_to',
        delta_col='fglbf.rate_delta',
        is_sorted=False
    )
    plnc = data[data['fglbf.notional_delta'] != 0]
    plrc = data[data['fglbf.rate_delta'] != 0]
    print(plrc)



if __name__ == '__main__':
    test_db_refresh_delta_by_to()