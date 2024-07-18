import pandas as pd


def df_to_antd_table(df: pd.DataFrame, col_titles=None):
    if col_titles is None:
        col_titles = {}

    columns = []
    for col in df.columns.tolist():
        if col in col_titles:
            title = col_titles[col]
        else:
            title = col
        columns.append(
            {
                'key': col,
                'title': title,
                'dataIndex': col
            }
        )

    data = df.to_dict(orient='records')

    res = {
        'columns': columns,
        'dataSource': data
    }

    return res
    