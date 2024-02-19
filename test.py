from helper_function.hf_data import *

from functions import get_cst_pki
# 假设表格数据如下，这里使用列表表示
relation_table = get_cst_pki()[['TABLE_NAME', 'REFERENCED_TABLE_NAME']].values.tolist()

graph = get_graph(relation_table=relation_table)
# for item in get_related_nodes(graph=graph, node='project'):
#     print(item)

for item in topological_sort(relation_table=relation_table):
    print(item)