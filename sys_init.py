import os
import platform
import socket

import configparser
import traceback

import pandas as pd
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

    return engine, con, url


def refresh_table_info_to_db():
    pd.read_excel(
        os.path.join(PATH_ROOT, 'table_info.xlsx'),
        sheet_name='schemas'
    ).to_sql(
        name='schemas', con=DB_ENGINE_CORE, if_exists='replace', index=False
    )

    pd.read_excel(
        os.path.join(PATH_ROOT, 'table_info.xlsx'),
        sheet_name='tables'
    ).to_sql(
        name='tables', con=DB_ENGINE_CORE, if_exists='replace', index=False
    )

    pd.read_excel(
        os.path.join(PATH_ROOT, 'table_info.xlsx'),
        sheet_name='cols'
    ).to_sql(
        name='cols', con=DB_ENGINE_CORE, if_exists='replace', index=False
    )


def refresh_db_info():
    global DB_SCHEMAS_INFO
    global DB_TABLES_INFO
    global DB_COLS_INFO

    refresh_table_info_to_db()

    DB_SCHEMAS_INFO = pd.read_sql(sql=f'select * from `schemas`', con=DB_ENGINE_CORE)
    DB_TABLES_INFO = pd.read_sql(sql=f'select * from tables', con=DB_ENGINE_CORE)
    DB_COLS_INFO = pd.read_sql(sql=f'select * from cols', con=DB_ENGINE_CORE)


PATH_ROOT = os.path.dirname(__file__)
PATH_ADMIN_INI = os.path.join(PATH_ROOT, 'admin.ini')
PATH_CONFIG_INI = os.path.join(PATH_ROOT, 'config.ini')
PATH_SNAPSHOT = os.path.join(PATH_ROOT, 'snapshots')
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

DB_SCHEMA_CORE = f'{PROJECT_NAME}_core_{SYS_MODE}'
DB_ENGINE_CORE, DB_CON_CORE, DB_URL_CORE = connect_db(
    db_type=DB_TYPE,
    username=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    schema=DB_SCHEMA_CORE,
    charset=DB_CHARSET
)

DB_SCHEMAS_INFO = pd.DataFrame()
DB_TABLES_INFO = pd.DataFrame()
DB_COLS_INFO = pd.DataFrame()
refresh_db_info()

DB_ENGINE, DB_CON, DB_URL = connect_db(
    db_type=DB_TYPE,
    username=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    schema='',
    charset=DB_CHARSET
)
DB_INSP = inspect(DB_ENGINE)
