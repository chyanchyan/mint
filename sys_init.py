import os
import platform
import socket
import shutil

import configparser
import traceback

import pymysql
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect

if 'mint' in __name__.split('.'):
    from .settings import *
else:
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
            print(url)
            print('bugs in db url')
            traceback.format_exc()
            print(e)
            raise sqlalchemy.exc.OperationalError

    return engine, con, url


def get_con(schema_tag):
    engine, con, url = connect_db(
        db_type=DB_TYPE,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        schema=get_schema(schema_tag=schema_tag),
        charset=DB_CHARSET
    )

    return con


def refresh_table_info_to_db():

    table_info_path = os.path.join(PATH_PROJECT, 'table_info.xlsx')
    if not os.path.exists(table_info_path):
        table_info_path = os.path.join(PATH_ROOT, 'table_info.xlsx')
        if not os.path.exists(table_info_path):
            shutil.copy(
                src=os.path.join(PATH_ROOT, 'templates', 'table_info_template.xlsx'),
                dst=table_info_path
            )

    schemas = pd.read_excel(
        table_info_path,
        sheet_name='schemas'
    )
    schemas.to_sql(
        name='schemas', con=DB_ENGINE_CORE, if_exists='replace', index=False
    )

    tables = pd.read_excel(
        table_info_path,
        sheet_name='tables'
    )
    tables.to_sql(
        name='tables', con=DB_ENGINE_CORE, if_exists='replace', index=False
    )

    cols = pd.read_excel(
        table_info_path,
        sheet_name='cols'
    )
    cols.to_sql(
        name='cols', con=DB_ENGINE_CORE, if_exists='replace', index=False
    )
    return schemas, tables, cols


def refresh_db_info():
    global DB_SCHEMAS_INFO
    global DB_TABLES_INFO
    global DB_COLS_INFO

    DB_SCHEMAS_INFO = pd.read_sql(sql=f'select * from `schemas`', con=DB_ENGINE_CORE)
    DB_TABLES_INFO = pd.read_sql(sql=f'select * from tables', con=DB_ENGINE_CORE)
    DB_COLS_INFO = pd.read_sql(sql=f'select * from cols', con=DB_ENGINE_CORE)
    DB_SCHEMAS_INFO['schema'] = DB_SCHEMAS_INFO['schema_tag'].apply(
        lambda x: get_schema(x)
    )


def get_schema(schema_tag):
    if SYS_MODE == 'PROD':
        return f'{PROJECT_NAME}_{schema_tag}'
    else:
        return f'{PROJECT_NAME}_{schema_tag}_{SYS_MODE}'


PATH_ROOT = os.path.dirname(__file__)
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

DB_SCHEMA_CORE = get_schema('core')
print(DB_SCHEMA_CORE)
DB_ENGINE_CORE, DB_CON_CORE, DB_URL_CORE = connect_db(
    db_type=DB_TYPE,
    username=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    schema=DB_SCHEMA_CORE,
    charset=DB_CHARSET
)

try:
    DB_SCHEMA_TAGS = pd.read_sql(
        f'select schema_tag from {get_schema("core")}.schemas',
        con=DB_ENGINE_CORE
    )['schema_tag'].tolist()
except sqlalchemy.exc.ProgrammingError as e:
    if e.orig.args[0] == 1146:
        print(e.args)
        print('creating...')
        refresh_table_info_to_db()
        DB_SCHEMA_TAGS = pd.read_sql(
            f'select schema_tag from {get_schema("core")}.schemas',
            con=DB_ENGINE_CORE
        )['schema_tag'].tolist()


DB_SCHEMAS = {}
DB_ENGINES = {}
DB_CONS = {}
DB_URLS = {}

for TAG in DB_SCHEMA_TAGS:
    DB_SCHEMAS[TAG] = get_schema(schema_tag=TAG)
    DB_ENGINES[TAG], DB_CONS[TAG], DB_URLS[TAG] = connect_db(
        db_type=DB_TYPE,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        schema=get_schema(schema_tag=TAG),
        charset=DB_CHARSET
    )

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

DB_SCHEMAS_INFO, DB_TABLES_INFO, DB_COLS_INFO = refresh_table_info_to_db()


if __name__ == '__main__':
    print(PATH_SNAPSHOT)
