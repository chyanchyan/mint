import os
import platform
import socket

import configparser
import traceback

import pymysql
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect

from settings import *


def connect_db(db_type, username, password, host, port, schema, charset, create_if_not_exist=True):

    url = str(
        f'{db_type}://{username}:{password}@{host}:{port}/'
        f'{schema}?charset={charset}&autocommit=true'
    )

    engine = create_engine(url=url)
    try:
        con = engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        if e.orig.args[0] == 1049:
            print(f'schema "{schema}" doesnt exist.')
            if create_if_not_exist:
                print('creating...')
                pymysql.connect(
                    host=host,
                    port=int(port),
                    user=username,
                    password=password,
                    charset=charset
                ).cursor().execute(f'create schema {schema}')
                print(f'schema "{schema}" created')
                engine = create_engine(url=url)
                con = engine.connect()
            else:
                traceback.format_exc()
                print(e)
                raise e
        else:
            print('bugs in db url')
            traceback.format_exc()
            print(e)
            raise sqlalchemy.exc.OperationalError

    return engine, con


PATH_ROOT = os.path.dirname(__file__)
PATH_ADMIN_INI = os.path.join(PATH_ROOT, 'admin.ini')
PATH_CONFIG_INI = os.path.join(PATH_ROOT, 'config.ini')
PATH_SNAPSHOT = os.path.join(PATH_ROOT, 'snapshots')
PATH_TABLE_INFO_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'table_info')
PATH_META_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'meta')
PATH_MODEL_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'models')
PATH_DB_SNAPSHOT = os.path.join(PATH_SNAPSHOT, 'db')

ADMIN = configparser.ConfigParser()
CONF = configparser.ConfigParser()
ADMIN.read(PATH_ADMIN_INI)
CONF.read(PATH_ADMIN_INI)

OS_TYPE = str.lower(platform.system())
PROJECT_NAME = CONF.get('SYS', 'project_name')
HOST_NAME = socket.gethostname()
TEST_HOST_NAMES = ADMIN.get('SYS', 'test_host_names').split()

if HOST_NAME in TEST_HOST_NAMES:
    SYS_MODE = 'TEST'
else:
    SYS_MODE = 'PROD'

DB_HOST = CONF.get(SYS_MODE, 'db_host')
DB_PORT = CONF.get(SYS_MODE, 'db_port')
DB_USERNAME = ADMIN.get(SYS_MODE, 'db_username')
DB_PASSWORD = ADMIN.get(SYS_MODE, 'db_password')
DB_TYPE = CONF.get(SYS_MODE, 'db_type')
DB_CHARSET = CONF.get(SYS_MODE, 'db_charset')
DB_SCHEMA = f'{PROJECT_NAME}_{SYS_MODE}'
DB_SCHEMA_ADMIN = f'{PROJECT_NAME}_admin_{SYS_MODE}'

DB_ENGINE, DB_CON = connect_db(
    db_type=DB_TYPE,
    username=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    schema=DB_SCHEMA,
    charset=DB_CHARSET
)
DB_ENGINE_ADMIN, DB_CONN_ADMIN = connect_db(
    db_type=DB_TYPE,
    username=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    schema=DB_SCHEMA_ADMIN,
    charset=DB_CHARSET
)
DB_INSP = inspect(DB_ENGINE)
