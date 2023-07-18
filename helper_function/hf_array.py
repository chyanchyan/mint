# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import copy
from functools import reduce
from copy import copy


def get_related_names(root, relations: dict):
    all_names = list(relations.keys())
    if root not in all_names:
        return set()

    if len(relations) == 0:
        return set(root)

    sub_relations = dict(relations)

    root_related = relations[root]
    sub_relations.pop(root)
    res = {root}
    for name in root_related:
        res |= get_related_names(root=name, relations=sub_relations)

    return res


def merge_series(a: pd.Series, v: pd.Series, how='update'):
    """
    how = fill, update
    """

    res = a.copy()
    for i, item in a.items():
        if pd.isna(item):
            try:
                res[i] = v[i]
            except KeyError:
                pass
        else:
            if how == 'fill':
                res[i] = item
            elif how == 'update':
                try:
                    if not pd.isna(v[i]):
                        res[i] = v[i]
                except KeyError:
                    pass
                else:
                    res[i] = item

    return res


def value_change_and_change_to_merge(a_change, a_change_to, init_value=0):
    if len(a_change) != len(a_change_to):
        print('change length %s is not = change_to length %s' % (len(a_change), len(a_change_to)))
        raise ValueError

    v0 = copy.copy(init_value)
    re_a_change = []
    re_a_change_to = []
    for index, item in enumerate(a_change):
        if pd.isna(item):
            if pd.isna(a_change_to[index]):
                print('missing value at index %s' % str(index))
                raise ValueError
            else:
                re_a_change.append(a_change_to[index] - v0)
                re_a_change_to.append(a_change_to[index])
                v0 = copy.copy(a_change_to[index])
        else:
            v0 += item
            re_a_change.append(item)
            re_a_change_to.append(v0)

    return re_a_change, re_a_change_to


def df_to_list_of_dict(df: pd.DataFrame):
    res = []
    for i, r in df.iterrows():
        res.append(r.to_dict())
    return res


def prod(array):
    return reduce(lambda x, y: x * y, array)


def test_value_change_and_change_to_merge():
    a_c = [1, 2, pd.NaT, 4, 5, 6]
    a_c_t = [pd.NaT, pd.NaT, 10, pd.NaT, pd.NaT, pd.NaT]

    print(value_change_and_change_to_merge(a_c, a_c_t, init_value=0))


def test_merge_series():
    a = pd.Series({'1': 'a', '2': 'b', '3': 'c', '4': np.nan, '5': 'e', '6': 'f'})
    v = pd.Series({'1': np.nan, '2': 'bb', '3': 'c', '4': 'd', '5': np.nan})

    res = merge_series(a, v, 'update')
    print(res)


def test_get_related_names():
    root = 'i'
    relations = {
        'a': [],
        'b': [],
        'c': [],
        'd': [],
        'e': [],
        'f': [],
        'g': ['a', 'b', 'c', 'd', 'e', 'f'],
        'h': ['a', 'c', 'd', 'e', 'f'],
        'i': ['g'],
        'j': ['h']
    }

    print(sorted(list(get_related_names(root=root, relations=relations))))


if __name__ == '__main__':
    test_get_related_names()