import getopt
import logging
import sys
import datetime
import functools
from xmlrpc import server
import pyodbc


CONFIG = None


def _init(config):
    global CONFIG
    CONFIG = config


def get_config_file() -> str:
    config_file = 'config.ini'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i")
        print(f"opts: {opts} args: {args}")
        if args:
            config_file = args[0]
            print(f"config file: {config_file}")
        else:
            print("no config file")
    except getopt.GetoptError:
        print('reading ini file error')
        logging.error('Reading ini file error')
        return ""
    return config_file


@functools.cache
def get_sql_config() -> tuple[str, str, str, str]:
    server = 'localhost'
    database = 'ShiftSchedule'
    username = 'sa'
    password = 'Btrust123'
    if CONFIG and 'sqlserver' in CONFIG:
        username = CONFIG['sqlserver']['name']
        password = CONFIG['sqlserver']['password']
        server = CONFIG['sqlserver']['host']
        database = CONFIG['sqlserver']['database']
        print(
            f"read ini file ok username is {username} password is {password} server is {server} database is {database}"
        )
    return (username, password, server, database)


def set_logging() -> None:
    file = datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]
    directory = '.\\'
    if CONFIG and 'logdirectory' in CONFIG:
        directory = CONFIG['logdirectory']['directory']

    logging.basicConfig(filename=directory + file + '.log', encoding='utf-8', level=logging.DEBUG)
    print(f"log directory: {directory + file + '.log'}")
    logging.info('Start......')


# def get_db():
#     (username, password, server, database) = get_sql_config()
#     connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
#     conn = pyodbc.connect(connection_string)
#     return conn
def get_sqlserver_driver():
    drivers = pyodbc.drivers()
    if "ODBC Driver 18 for SQL Server" in drivers:
        return "ODBC Driver 18 for SQL Server", True
    if "ODBC Driver 17 for SQL Server" in drivers:
        return "ODBC Driver 17 for SQL Server", False
    raise RuntimeError("No supported SQL Server ODBC Driver found")

class DBContext:
    def __enter__(self):
        driver, is18 = get_sqlserver_driver()
        (username, password, server, database) = get_sql_config()
        connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
        # Driver 18 默认强制加密，必须加这个
        if is18:
            connection_string += "Encrypt=yes;TrustServerCertificate=yes;"
        self.conn = pyodbc.connect(connection_string)
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()
