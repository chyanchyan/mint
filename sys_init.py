"""
系统初始化模块

该模块负责数据库连接管理、表结构信息刷新、模型生成等系统初始化功能。
主要功能包括：
1. 数据库连接管理
2. 表结构信息从Excel文件同步到数据库
3. 动态生成表对象和SQLAlchemy模型
4. 数据库表创建
"""

import shutil
import os
import sys
from sqlalchemy import text

# 设置Python路径，确保可以导入父目录的模块
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 导入项目相关模块
import mint.globals as glb
from mint.globals import *
from mint.db.utils import *
from mint.helper_function.hf_string import to_json_str, list_to_attr_code
from mint.helper_function.wrappers import sub_wrapper
from mint.meta.table_objs import get_tables_from_info
from mint.meta import models


def get_schema(schema_tag):
    """
    根据schema标签获取对应的schema名称
    
    Args:
        schema_tag (str): schema标签
        
    Returns:
        str: 对应的schema名称
    """
    res = db_get_schema(schema_tag=schema_tag, sys_mode=SYS_MODE, project_name=PROJECT_NAME)
    return res


def get_con(schema_tag=None, auto_commit=True):
    """
    获取数据库连接对象
    
    Args:
        schema_tag (str, optional): schema标签，默认为None
        auto_commit (bool): 是否自动提交，默认为True
        
    Returns:
        Connection: 数据库连接对象
    """
    return get_engine_con_url(schema_tag=schema_tag, auto_commit=auto_commit)[1]

def get_engine(schema_tag=None, auto_commit=True):
    """
    获取数据库引擎对象
    
    Args:
        schema_tag (str, optional): schema标签，默认为None
        auto_commit (bool): 是否自动提交，默认为True
        
    Returns:
        Engine: 数据库引擎对象
    """
    return get_engine_con_url(schema_tag=schema_tag, auto_commit=auto_commit)[0]


def get_fg_data_con():
    """
    获取FG（financial guarantee）数据源的数据库连接
    
    Returns:
        Connection: FG（financial guarantee）数据源的数据库连接对象
    """
    host = DB_PARAMS['db_fg_host']
    port = DB_PARAMS['db_fg_port']
    username = DB_PARAMS['db_fg_username']
    password = DB_PARAMS['db_fg_password']
    schema = 'soams_dxm'
    charset = DB_PARAMS['db_fg_charset']
    url = str(
        f'mysql+pymysql://{username}:{password}@{host}:{port}/'
        f'{schema}?charset={charset}&autocommit=true'
    )
    engine = create_engine(url=url)
    con = engine.connect()
    return con


def get_engine_con_url(schema_tag=None, auto_commit=True):
    """
    获取数据库引擎、连接和URL
    
    Args:
        schema_tag (str, optional): schema标签，默认为None
        auto_commit (bool): 是否自动提交，默认为True
        
    Returns:
        tuple: (engine, connection, url) 元组
    """
    if schema_tag is None:
        return db_connect_db(
            **DB_PARAMS,
            schema='',
            auto_commit=auto_commit
        )
    else:
        return db_connect_db(
            **DB_PARAMS,
            schema=get_schema(schema_tag=schema_tag),
            auto_commit=auto_commit
        )


def get_schema_tags():
    """
    获取所有可用的schema标签
    
    Returns:
        list: schema标签列表
    """
    con_core = get_con('core')
    schema_tags = db_get_schema_tags(con=con_core)
    con_core.close()
    return schema_tags


def refresh_table_info_to_db():
    """
    将Excel文件中的表结构信息刷新到数据库
    
    该函数会：
    1. 读取table_info.xlsx文件中的schemas、tables、cols三个工作表
    2. 将数据写入到数据库对应的表中
    3. 如果文件不存在，会从模板复制一份
    
    Returns:
        tuple: (schemas, tables, cols) 三个DataFrame对象
    """
    con = get_con('core')
    table_info_path = os.path.join(PATH_PROJECT, 'table_info.xlsx')
    if not os.path.exists(table_info_path):
        table_info_path = os.path.join(PATH_ROOT, 'table_info.xlsx')
        if not os.path.exists(table_info_path):
            # 如果文件不存在，从模板复制
            shutil.copy(
                src=os.path.join(PATH_ROOT, 'templates', 'table_info_template.xlsx'),
                dst=table_info_path
            )

    # 读取并写入schemas表
    schemas = pd.read_excel(
        table_info_path,
        sheet_name='schemas'
    )
    schemas.to_sql(
        name='schemas', con=con, if_exists='replace', index=False
    )

    # 读取并写入tables表
    tables = pd.read_excel(
        table_info_path,
        sheet_name='tables'
    )
    tables.to_sql(
        name='tables', con=con, if_exists='replace', index=False
    )

    # 读取并写入cols表
    cols = pd.read_excel(
        table_info_path,
        sheet_name='cols'
    )
    cols.to_sql(
        name='cols', con=con, if_exists='replace', index=False
    )
    return schemas, tables, cols


@sub_wrapper(SYS_MODE)
def refresh_db_info(con):
    """
    刷新数据库信息到全局变量
    
    该函数会从数据库读取最新的表结构信息，并更新全局变量：
    - DB_SCHEMAS_INFO: 数据库schema信息
    - DB_TABLES_INFO: 数据库表信息  
    - DB_COLS_INFO: 数据库列信息
    
    Args:
        con: 数据库连接对象
    """
    global DB_SCHEMAS_INFO
    global DB_TABLES_INFO
    global DB_COLS_INFO

    DB_SCHEMAS_INFO = pd.read_sql(sql=f'select * from `schemas`', con=con)
    DB_TABLES_INFO = pd.read_sql(sql=f'select * from tables', con=con)
    DB_COLS_INFO = pd.read_sql(sql=f'select * from cols', con=con)
    # 为schema信息添加实际的schema名称
    DB_SCHEMAS_INFO['schema'] = DB_SCHEMAS_INFO['schema_tag'].apply(
        lambda x: get_schema(x)
    )


@sub_wrapper(SYS_MODE)
def refresh_table_obj():
    """
    刷新表对象文件
    
    该函数会根据数据库中的表结构信息，动态生成table_objs.py文件。
    生成的文件包含所有表和列的属性定义，用于ORM操作。
    """
    template_pth = os.path.join(PATH_ROOT, 'meta', 'templates', 'table_objs.py')
    output_pth = os.path.join(PATH_ROOT, 'meta', 'table_objs.py')
    code = open(template_pth, 'r').readlines()

    # 生成列属性代码
    st_mark = ' ' * 8 + '# col attr start\n'
    ed_mark = ' ' * 8 + '# col attr end\n'
    code = list_to_attr_code(
        code_template=code,
        attr_list=DB_COLS_INFO.columns.to_list(),
        df_var_name='col_info',
        st_mark=st_mark,
        ed_mark=ed_mark,
        intent_blocks=2
    )
    
    # 生成表属性代码
    st_mark = ' ' * 12 + '# table attr start\n'
    ed_mark = ' ' * 12 + '# table attr end\n'
    code = list_to_attr_code(
        code_template=code,
        attr_list=DB_TABLES_INFO.columns.to_list(),
        df_var_name='table_info',
        st_mark=st_mark,
        ed_mark=ed_mark,
        intent_blocks=3
    )
    
    # 写入生成的文件
    f = open(output_pth, 'w')
    f.writelines(code)
    f.close()


@sub_wrapper(SYS_MODE)
def refresh_models():
    """
    刷新SQLAlchemy模型文件
    
    该函数会根据数据库中的表结构信息，动态生成models.py文件。
    生成的文件包含所有表的SQLAlchemy模型类定义。
    """
    # 加载模型文件模板
    template_pth = os.path.join(PATH_ROOT, 'meta', 'templates', 'models.py')
    output_pth = os.path.join(PATH_ROOT, 'meta', 'models.py')
    f = open(template_pth, encoding='utf-8', mode='r')
    rs = f.readlines()
    st_str = '# table class start\n'
    ed_str = '# table class end\n'
    pre_block, post_block = rs[: rs.index(st_str) + 1], rs[rs.index(ed_str):]

    # 获取表信息并生成模型代码
    tables = get_tables_from_info(
        tables_info=DB_TABLES_INFO,
        cols_info=DB_COLS_INFO,
        get_schema=get_schema
    )
    tables_list = [v for k, v in tables.items()]
    tables_list.sort(key=lambda x: x.order)  # 按order排序
    table_blocks = []
    for table in tables_list:
        table_blocks.append(table.to_model_code())
        table_blocks.extend(['\n', '\n'])

    # 组合完整的文件内容
    res = pre_block + table_blocks + post_block

    # 写入文件
    f = open(output_pth, encoding='utf-8', mode='w')
    f.writelines(res)
    f.close()


@sub_wrapper(SYS_MODE)
def create_tables():
    """
    创建数据库表
    
    该函数会根据SQLAlchemy模型定义，在数据库中创建所有表。
    如果schema不存在，会自动创建schema。
    """
    engine, con, url = get_engine_con_url('data')
    engine = create_engine(url)
    print("creating tables")
    while True:
        try:
            models.Base.metadata.create_all(engine)
            break
        except sqlalchemy.exc.OperationalError as e:
            if e.orig.args[0] == 1049:  # MySQL错误码：数据库不存在
                schema = e.orig.args[1].split("'")[1]
                print(f'schema "{schema}" doesnt exist.')
                print('creating...')
                con.execute(text(f'create schema {schema}'))
                print(f'schema "{schema}" created')
            else:
                raise e
    print("tables created")
    con.close()

# 模块执行时的初始化逻辑
print(__name__)
if __name__ == 'api.py':
    # 当作为api.py模块导入时，执行完整的初始化流程
    DB_SCHEMAS_INFO, DB_TABLES_INFO, DB_COLS_INFO = refresh_table_info_to_db()
    refresh_table_obj()
    refresh_models()
    create_tables()

# 获取数据表对象并打印主机名
TABLES = get_tables('data')
print(f'host name: {HOST_NAME}')

