"""
数据库元数据管理模块

该模块提供了数据库schema管理、快照备份、系统恢复等功能。
主要用于数据库结构的创建、删除、备份和恢复操作。
"""

import os.path
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, create_engine

import os
import sys

# 获取当前文件所在目录的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 获取父目录路径
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到Python路径中，以便导入mint模块
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 导入系统初始化模块和辅助函数
from mint.sys_init import *
from mint.helper_function.wrappers import sub_wrapper
from mint.helper_function.hf_file import snapshot, mkdir

from mint.helper_function.hf_db import export_xl


@sub_wrapper(SYS_MODE)
def drop_schemas(schema_tags=None):
    """
    删除并重新创建指定的数据库schema
    
    Args:
        schema_tags: schema标签列表，如果为None则使用默认的schema标签
    """
    # 获取数据库连接
    engine, con, url = get_engine_con_url()
    session_class = sessionmaker(bind=engine)
    session = session_class()

    # 如果没有指定schema标签，则使用默认的
    if schema_tags is None:
        schema_tags = get_schema_tags()

    # 遍历每个schema标签，删除并重新创建对应的schema
    for schema_tag in schema_tags:
        schema = get_schema(schema_tag=schema_tag)
        drop_schema(session=session, schema=schema)
    
    # 提交事务并关闭连接
    session.commit()
    session.close()
    con.close()


@sub_wrapper(SYS_MODE)
def drop_schema(session, schema):
    """
    删除并重新创建单个数据库schema
    
    Args:
        session: 数据库会话对象
        schema: 要操作的schema名称
    """
    # 删除schema（如果存在）
    session.execute(
        text(f'drop database if exists {schema}')
    )
    # 重新创建schema
    session.execute(
        text(f'create database {schema}')
    )


@sub_wrapper(SYS_MODE)
def snapshot_table_obj():
    """
    为table_objs.py文件创建快照备份
    """
    snapshot(src_path='table_objs.py', dst_folder=PATH_META_SNAPSHOT, auto_timestamp=True, comments='')


@sub_wrapper(SYS_MODE)
def snapshot_models():
    """
    为models.py文件创建快照备份
    """
    snapshot(src_path='models.py', dst_folder=PATH_MODEL_SNAPSHOT, auto_timestamp=True, comments='')


def snapshot_database(schema_tags=None, comments=''):
    """
    为数据库创建快照备份，将数据库表导出为Excel文件
    
    Args:
        schema_tags: schema标签列表，如果为None则使用默认的schema标签
        comments: 快照的注释信息
    """
    # 如果没有指定schema标签，则使用默认的
    if schema_tags is None:
        schema_tags = get_schema_tags()
    
    # 创建带时间戳的文件夹名称
    folder = dt.now().strftime('%Y%m%d_%H%M%S_%f') + f'-{comments}'
    
    # 确保数据库快照目录存在
    if not os.path.exists(PATH_DB_SNAPSHOT):
        mkdir(PATH_DB_SNAPSHOT)

    # 获取数据库连接
    con = get_con()

    # 为每个schema创建快照
    for schema_tag in schema_tags:
        # 创建schema特定的文件夹路径
        folder = os.path.join(
            str(PATH_DB_SNAPSHOT),
            str(folder),
            str(get_schema(schema_tag))
        )
        
        # 确保文件夹存在
        if not os.path.exists(folder):
            mkdir(folder)

        # 导出数据库表为Excel文件
        export_xl(
            output_folder=folder,
            con=con,
            schema=get_schema(schema_tag=schema_tag),
            table_names=None
        )


@sub_wrapper(SYS_MODE)
def restore_sys():
    """
    完全恢复系统：删除所有schema，重新创建表结构，刷新元数据
    """
    # 删除并重新创建所有schema
    drop_schemas()
    # 刷新表信息到数据库
    refresh_table_info_to_db()

    # 刷新核心数据库信息
    con = get_con('core')
    refresh_db_info(con=con)
    con.close()

    # 创建table_objs.py的快照并刷新
    snapshot_table_obj()
    refresh_table_obj()
    # 创建models.py的快照并刷新
    snapshot_models()
    refresh_models()
    # 创建所有表
    create_tables()


@sub_wrapper(SYS_MODE)
def restore_table():
    """
    恢复表结构：刷新表信息，不删除现有数据
    """
    # 刷新表信息到数据库
    refresh_table_info_to_db()

    # 刷新核心数据库信息
    con = get_con('core')
    refresh_db_info(con=con)
    con.close()

    # 创建table_objs.py的快照并刷新
    snapshot_table_obj()
    refresh_table_obj()


@sub_wrapper(SYS_MODE)
def restore_db(schema_tags=None):
    """
    恢复数据库：删除并重新创建指定的schema和表
    
    Args:
        schema_tags: schema标签列表，如果为None则使用默认的schema标签
    """
    # 删除并重新创建schema
    drop_schemas(schema_tags)
    # 创建所有表
    create_tables()


@sub_wrapper(SYS_MODE)
def add_table():
    """
    添加新表：刷新表信息，创建新表，不删除现有数据
    """
    # 刷新表信息到数据库
    refresh_table_info_to_db()

    # 刷新核心数据库信息
    con = get_con('core')
    refresh_db_info(con=con)
    con.close()

    # 创建models.py的快照并刷新
    snapshot_models()
    refresh_models()
    # 创建新表
    create_tables()


def init_sys():
    """
    初始化系统：刷新表信息、模型和表对象
    """
    refresh_table_info_to_db()
    refresh_models()
    refresh_table_obj()


# 模块加载时的自动初始化
# 如果table_obj.py不存在，则刷新表对象
if not os.path.exists('table_obj.py'):
    refresh_table_obj()

# 如果models.py不存在，则刷新模型
if not os.path.exists('models.py'):
    refresh_models()
