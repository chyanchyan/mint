import os
import platform
import socket

import configparser

PATH_ROOT = os.path.dirname(os.path.dirname(__file__))
PATH_PROJECT = os.path.dirname(PATH_ROOT)

PATH_ADMIN_INI = os.path.join(os.path.dirname(PATH_ROOT), 'admin.ini')
if not os.path.exists(PATH_ADMIN_INI):
    PATH_ADMIN_INI = os.path.join(PATH_ROOT, 'admin.ini')

PATH_CONFIG_INI = os.path.join(os.path.dirname(PATH_ROOT), 'config.ini')
if not os.path.exists(PATH_CONFIG_INI):
    PATH_CONFIG_INI = os.path.join(PATH_ROOT, 'config.ini')

PATH_SNAPSHOT = os.path.join(PATH_PROJECT, 'snapshots')
PATH_OUTPUT = os.path.join(PATH_PROJECT, 'output')
PATH_UPLOAD = os.path.join(PATH_PROJECT, 'upload')
PATH_TABLE_INFO_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'table_info')
PATH_META_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'meta')
PATH_MODEL_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'models')
PATH_DB_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'db')

CONF_ADMIN = configparser.ConfigParser()
CONF_CONF = configparser.ConfigParser()
CONF_ADMIN.read(PATH_ADMIN_INI)
CONF_CONF.read(PATH_CONFIG_INI)

OS_TYPE = str.lower(platform.system())
PROJECT_NAME = CONF_CONF.get('SYS', 'project_name')
HOST_NAME = socket.gethostname()
TEST_HOST_NAMES = CONF_ADMIN.get('SYS', 'test_host_names').split()

print(f'host name: {HOST_NAME}')
if HOST_NAME in TEST_HOST_NAMES:
    SYS_MODE = 'TEST'
else:
    SYS_MODE = 'PROD'

DB_HOST = CONF_CONF.get(SYS_MODE, 'db_host')
DB_PORT = CONF_CONF.get(SYS_MODE, 'db_port')
DB_USERNAME = CONF_ADMIN.get(SYS_MODE, 'db_username')
DB_PASSWORD = CONF_ADMIN.get(SYS_MODE, 'db_password')
DB_TYPE = CONF_CONF.get(SYS_MODE, 'db_type')
DB_CHARSET = CONF_CONF.get(SYS_MODE, 'db_charset')

DB_PARAMS = dict(CONF_CONF[SYS_MODE].items())
DB_PARAMS.update(dict(CONF_ADMIN[SYS_MODE].items()))
