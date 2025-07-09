# 导入必要的系统模块
import os
import platform
import socket

# 导入配置文件解析模块
import configparser

# ==================== 路径配置 ====================
# 获取当前文件所在目录路径（mint目录）
PATH_ROOT = os.path.dirname(__file__)
# 获取项目根目录路径（vision目录）
PATH_PROJECT = os.path.dirname(PATH_ROOT)

# 配置admin.ini文件路径，优先查找项目根目录，如果不存在则查找mint目录
PATH_ADMIN_INI = os.path.join(os.path.dirname(PATH_ROOT), 'admin.ini')
if not os.path.exists(PATH_ADMIN_INI):
    PATH_ADMIN_INI = os.path.join(PATH_ROOT, 'admin.ini')

# 配置config.ini文件路径，优先查找项目根目录，如果不存在则查找mint目录
PATH_CONFIG_INI = os.path.join(os.path.dirname(PATH_ROOT), 'config.ini')
if not os.path.exists(PATH_CONFIG_INI):
    PATH_CONFIG_INI = os.path.join(PATH_ROOT, 'config.ini')

# ==================== 项目目录路径配置 ====================
# 快照目录路径
PATH_SNAPSHOT = os.path.join(PATH_PROJECT, 'snapshots')
# 输出目录路径
PATH_OUTPUT = os.path.join(PATH_PROJECT, 'output')
# 上传目录路径
PATH_UPLOAD = os.path.join(PATH_PROJECT, 'upload')
# 表信息快照目录路径
PATH_TABLE_INFO_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'table_info')
# 元数据快照目录路径
PATH_META_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'meta')
# 模型快照目录路径
PATH_MODEL_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'models')
# 数据库快照目录路径
PATH_DB_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'db')

# ==================== 配置文件解析 ====================
# 创建管理员配置解析器实例
CONF_ADMIN = configparser.ConfigParser()
# 创建系统配置解析器实例
CONF_CONF = configparser.ConfigParser()
# 读取管理员配置文件
CONF_ADMIN.read(PATH_ADMIN_INI)
# 读取系统配置文件
CONF_CONF.read(PATH_CONFIG_INI)

# ==================== 系统环境检测 ====================
# 获取操作系统类型（转换为小写）
OS_TYPE = str.lower(platform.system())
# 从配置文件获取项目名称
PROJECT_NAME = CONF_CONF.get('SYS', 'project_name')
# 获取当前主机名
HOST_NAME = socket.gethostname()
# 从配置文件获取测试环境主机名列表
TEST_HOST_NAMES = CONF_ADMIN.get('SYS', 'test_host_names').split()

# 根据主机名判断系统运行模式
if HOST_NAME in TEST_HOST_NAMES:
    SYS_MODE = 'TEST'  # 测试环境
else:
    SYS_MODE = 'PROD'  # 生产环境

# ==================== 数据库配置 ====================
# 根据运行模式获取数据库配置参数
DB_HOST = CONF_CONF.get(SYS_MODE, 'db_host')        # 数据库主机地址
DB_PORT = CONF_CONF.get(SYS_MODE, 'db_port')        # 数据库端口
DB_USERNAME = CONF_ADMIN.get(SYS_MODE, 'db_username')  # 数据库用户名
DB_PASSWORD = CONF_ADMIN.get(SYS_MODE, 'db_password')  # 数据库密码
DB_TYPE = CONF_CONF.get(SYS_MODE, 'db_type')        # 数据库类型
DB_CHARSET = CONF_CONF.get(SYS_MODE, 'db_charset')  # 数据库字符集

# 合并系统配置和管理员配置中的数据库参数
DB_PARAMS = dict(CONF_CONF[SYS_MODE].items())
DB_PARAMS.update(dict(CONF_ADMIN[SYS_MODE].items()))

