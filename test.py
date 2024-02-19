import pandas as pd

# 创建两个DataFrame
df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
df2 = pd.DataFrame({'A': [None, 8, None], 'B': [None, None, 12]})

# 使用combine_first方法
result = df1.combine_first(df2)
print(result)