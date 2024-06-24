import pandas as pd
import numpy as np

from mint.helper_function.hf_math import get_group_last_row_before_line

def new(
        data,
        index,
        line_col_name,
        lines
):
    data_sorted = data.sort_values(index).reset_index(drop=True)
    line_values = data_sorted[line_col_name].values[:, np.newaxis]

    lines_array = np.array(lines)

    # 创建布尔矩阵，表示哪些值小于线
    a = line_values < lines_array

    # 按照分组计算每组在每条线之前的最后一行的索引
    group_idxs = data_sorted.groupby(index).cumcount().values[:, np.newaxis]
    max_idxs = np.where(a, group_idxs, -1).max(axis=0)

    # 初始化结果矩阵
    res = np.zeros_like(a, dtype=int)

    # 使用布尔索引设置结果矩阵
    valid_mask = max_idxs >= 0
    res[max_idxs[valid_mask], np.arange(len(lines))[valid_mask]] = 1

    return res


data = pd.read_excel('before_st_test_data.xlsx')
res1 = get_group_last_row_before_line(
    data,
    index='name',
    line_col_name='date',
    line_values=data['date'].values[:, np.newaxis],
    lines=np.array([3, 4, 5])
)
print(res1)
# res2 = new(
#     data=data,
#     index='name',
#     line_col_name='date',
#     lines=[3, 4, 5]
# )