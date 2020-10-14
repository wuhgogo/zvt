# -*- coding: utf-8 -*-
import json
import logging
import os
from logging.handlers import RotatingFileHandler

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists

from zvt.settings import DATA_SAMPLE_ZIP_PATH, ZVT_TEST_HOME, ZVT_HOME, ZVT_TEST_DATA_PATH, ZVT_TEST_ZIP_DATA_PATH
from zvt.utils.zip_utils import unzip


def init_log(file_name='zvt.log', log_dir=None, simple_formatter=True):
    if not log_dir:
        log_dir = zvt_env['log_path']

    root_logger = logging.getLogger()

    # reset the handlers
    root_logger.handlers = []

    root_logger.setLevel(logging.INFO)

    file_name = os.path.join(log_dir, file_name)

    fh = RotatingFileHandler(file_name, maxBytes=524288000, backupCount=10)

    fh.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    if simple_formatter:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s  %(threadName)s  %(message)s")
    else:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s  %(threadName)s  %(name)s:%(filename)s:%(lineno)s  %(funcName)s  %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    root_logger.addHandler(fh)
    root_logger.addHandler(ch)


pd.set_option('expand_frame_repr', False)
pd.set_option('mode.chained_assignment', 'raise')

zvt_env = {}


def init_env(zvt_home: str) -> None:
    """

    :param zvt_home: home path for zvt
    """
    data_path = os.path.join(zvt_home, 'data')
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    zvt_env['zvt_home'] = zvt_home
    zvt_env['data_path'] = data_path

    # path for storing ui results
    zvt_env['ui_path'] = os.path.join(zvt_home, 'ui')
    if not os.path.exists(zvt_env['ui_path']):
        os.makedirs(zvt_env['ui_path'])

    # path for storing logs
    zvt_env['log_path'] = os.path.join(zvt_home, 'logs')
    if not os.path.exists(zvt_env['log_path']):
        os.makedirs(zvt_env['log_path'])

    # create default config.json if not exist
    config_path = os.path.join(zvt_home, 'config.json')
    if not os.path.exists(config_path):
        from shutil import copyfile
        copyfile(os.path.abspath(os.path.join(os.path.dirname(__file__), 'samples', 'config.json')), config_path)

    with open(config_path) as f:
        config_json = json.load(f)
        for k in config_json:
            zvt_env[k] = config_json[k]

    init_log()

    import pprint
    pprint.pprint(zvt_env)


if os.getenv('TESTING_ZVT'):
    init_env(zvt_home=ZVT_TEST_HOME)

    # init the sample data if need
    same = False
    if os.path.exists(ZVT_TEST_ZIP_DATA_PATH):
        import filecmp

        same = filecmp.cmp(ZVT_TEST_ZIP_DATA_PATH, DATA_SAMPLE_ZIP_PATH)

    if not same:
        from shutil import copyfile

        copyfile(DATA_SAMPLE_ZIP_PATH, ZVT_TEST_ZIP_DATA_PATH)
        unzip(ZVT_TEST_ZIP_DATA_PATH, ZVT_TEST_DATA_PATH)

    if "db_engine" in zvt_env and "False" != zvt_env.get("db_test_copy"):
        if "True" == zvt_env.get("db_echo"):
            need_echo = True
        else:
            need_echo = False
        # foreach test data file
        test_data_dir = os.listdir(ZVT_TEST_DATA_PATH)
        for cur_file in test_data_dir:
            path = os.path.join(ZVT_TEST_DATA_PATH, cur_file)
            engine_key = os.path.splitext(cur_file)[0]
            file_suffix = os.path.splitext(cur_file)[1]
            if os.path.isfile(path) and file_suffix == ".db":
                # create db engine
                sqlite_path = os.path.join(ZVT_TEST_DATA_PATH, '{}.db?check_same_thread=False'.format(engine_key))
                sqlite_engine = create_engine('sqlite:///' + sqlite_path, echo=need_echo)
                db_url = f"{zvt_env['db_engine']}+mysqldb://{zvt_env['db_username']}:{zvt_env['db_password']}@{zvt_env['db_address']}:" f"{zvt_env['db_port']}/{engine_key}?charset=utf8mb4"
                # cause we use read_sql and to_sql to copy the data and no way to call this after register_schema
                # so the copy will after once running that create the database and table
                if database_exists(db_url):
                    mysql_engine = create_engine(db_url, pool_recycle=3600, echo=need_echo)
                    # copy db data
                    session = sessionmaker(bind=mysql_engine)
                    session = session()
                    Base = declarative_base()
                    Base.metadata.reflect(sqlite_engine)
                    tables = Base.metadata.tables
                    for table in tables:
                        # copy data to not exists table will create incompatible schema
                        # so copy action will after the schema create
                        if mysql_engine.dialect.has_table(mysql_engine, table):
                            sqlite_data = pd.read_sql(table, sqlite_engine)
                            mysql_data = pd.read_sql(table, mysql_engine)
                            if sqlite_data.shape[0] > 0 and mysql_data.shape[0] == 0:
                                try:
                                    print("copy {}.{} size {}".format(engine_key, table, sqlite_data.shape[0]))
                                    sqlite_data.to_sql(table, mysql_engine, index=False, if_exists='append')
                                    session.commit()
                                except Exception as e:
                                    print(e)
                                    session.rollback()


else:
    init_env(zvt_home=ZVT_HOME)

import zvt.domain as domain
import zvt.recorders as recorders

import pluggy

hookimpl = pluggy.HookimplMarker("zvt")
"""Marker to be imported and used in plugins (and for own implementations)"""

__all__ = ['domain', 'recorders', 'zvt_env', 'init_log', 'init_env']
