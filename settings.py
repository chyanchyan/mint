import os
import platform
import configparser
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

CONF = configparser.ConfigParser()

OS_TYPE = str.lower(platform.system())

SYS_MODE = 'test'

SERVER_HOST = ''
SERVER_BACKEND = ''

DB_HOST_PROD = '127.0.0.1'
DB_HOST_LOCAL = '127.0.0.1'
DB_HOST_TEST = '127.0.0.1'

PATH_ADMIN = 'admin.ini'
PATH_SNAPSHOTS = 'snapshots'

PORT_FLASK = 8000

DB_PORT_PROD = 8083
DB_PORT_LOCAL = 3306
DB_PORT_TEST = 3306

DB_TYPE = 'mysql'
DB_CHARSET = 'utf8'

PATH_SNAPSHOT = os.path.join('.', 'core', 'snapshots')
PATH_FIELD_PARAM_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'field_param')
PATH_META_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'meta')
PATH_MODEL_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'models')
PATH_DB_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'db')
PATH_ROOT = os.path.dirname(__file__)
PATH_ADMIN_INI = os.path.join(PATH_ROOT, 'admin.ini')
CONF.read(PATH_ADMIN_INI)

sys_mode = str.lower(SYS_MODE)
if sys_mode == 'test':
    DB_HOST = str(DB_HOST_TEST)
    DB_PORT = int(DB_PORT_TEST)
    DB_USERNAME = CONF.get('DB_ADMIN_TEST', 'username')
    DB_PASSWORD = CONF.get('DB_ADMIN_TEST', 'password')
elif sys_mode == 'local':
    DB_HOST = str(DB_HOST_LOCAL)
    DB_PORT = int(DB_PORT_LOCAL)
    DB_USERNAME = CONF.get('DB_ADMIN_LOCAL', 'username')
    DB_PASSWORD = CONF.get('DB_ADMIN_LOCAL', 'password')
elif sys_mode == 'prod' or sys_mode == 'production':
    DB_HOST = str(DB_HOST_PROD)
    DB_PORT = int(DB_PORT_PROD)
    DB_USERNAME = CONF.get('DB_ADMIN', 'username')
    DB_PASSWORD = CONF.get('DB_ADMIN', 'password')
else:
    DB_HOST = str(DB_HOST_TEST)
    DB_PORT = int(DB_PORT_TEST)
    DB_USERNAME = CONF.get('DB_ADMIN_TEST', 'username')
    DB_PASSWORD = CONF.get('DB_ADMIN_TEST', 'password')

DB_SCHEMA = 'mint_db'
DB_SCHEMA_CORE = 'mint_core'

DB_URL_BASE = f'{DB_TYPE}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/?charset={DB_CHARSET}'
DB_URL = f'{DB_TYPE}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_SCHEMA}?charset={DB_CHARSET}'
DB_URL_CORE = f'{DB_TYPE}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_SCHEMA_CORE}?charset={DB_CHARSET}'
