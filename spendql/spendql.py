import os
import sqlite3
import csv
import re

from . import tags

# TODO: create where, values, etc... methods that generate sanatized sql given user input in whatever form (string, dict, list...)

fdir = os.path.dirname(__file__).replace('\\', '/')

rxNonAlphaNumeric = re.compile('[^a-z0-9_]+', re.I | re.M)
rxPassiveSql = re.compile('AND|OR|\=|<|>|=', re.I | re.M)
rxDoubleDot = re.compile('\.\.', re.I | re.M)

open_conns = {}

# Path Functions, abstract away the storage details
def getPath(suffix):
    if suffix[0] != '/':
        suffix = '/' + suffix

    return fdir + suffix


def getDBPath(name):
    return getPath('/dbs/{0}.db'.format(name))


def getSQLPath(name):
    if rxDoubleDot.search(name) is None:
        name.replace('.', '/')
        return getPath('/sql/{0}.sql'.format(name))

# DB functions
def dropDB(name):
    path = getDBPath(name)
    if os.path.isfile(path):
        os.remove(path)

def dbExists(name):
    path = getDBPath(name)
    return os.path.isfile(path)

# connection functions, connections are pooled
def getConn(name):
    if not(name in open_conns):
        path = getDBPath(name)
        open_conns[name] = sqlite3.connect(path)

    return open_conns[name]


def closeConn(name):
    if name in open_conns:        
        open_conns[name].close()
        del open_conns[name]


def closeAllConns():
    for key in open_conns:
        closeConn(key)

# raw sql functions
# sanatize sql, if there is any dirty input return nothing
def readSqlFile(name):
    path = getSQLPath(name)
    with open(path, 'r') as sql:
        return sql.read()


def renderSql(sql, args):
    if isinstance(args, dict):
        return sql.format(**args)
    elif isinstance(args, list):
        return sql.format(*args)
    else:
        return sql.format(args)


def renderSqlFile(name, args):
    sql = readSqlFile(name)
    return renderSql(sql, args)


def sqlValue(val):
    if not(isinstance(val, int) or isinstance(val, float)):
        val = "'{}'".format(val)
    return val


def sqlValues(col_val):
    vals = list(col_val.values())

    vals_sql = [sqlValue(val) for val in vals]
    vals_str = ','.join(vals_sql)
    return 'VALUES ({})'.format(vals_str)


def sqlColumns(col_val):
    cols = list(col_val.keys())
    return '({})'.format(','.join(cols))

def sqlWhere(filters):
    where_clause = ''
    if isinstance(filters, dict):
        key_value = ['{}={}'.format(key, sqlValue(filters[key])) for key in filters]
        where_clause += ' AND '.join(key_value)

    elif isinstance(filters, str):
        where_clause += filters

    if where_clause != '':
        where_clause = 'WHERE ' + where_clause
    
    return where_clause


def sqlUpdateSet(values):
    key_value = ['{}={}'.format(key, sqlValue(values[key])) for key in values]
    return 'SET ' + ','.join(key_value)


def command(conn, name, str_params=[], sql_params=[]):
    cursor = conn.cursor()

    sql = renderSqlFile(name, str_params)

    if sql is not None:
        try:
            cursor.execute(sql, sql_params)
        except:
            pass

        conn.commit()
        return cursor


def script(conn, name, str_params=[]):
    cursor = conn.cursor()

    sql = renderSqlFile(name, str_params)

    if sql is not None:
        cursor.executescript(sql)
        conn.commit()
        return cursor


# Table operations
def dropTable(conn, table_name):
    return command(conn, 'drop', str_params=table_name)


def truncateTable(conn, table_name):
    return command(conn, 'truncate', str_params=table_name)


def dumpTable(conn, table_name):
    return command(conn, 'dump', str_params=table_name)


# record operations
def create(conn, table_name, record):
    cols = sqlColumns(record)
    vals = sqlValues(record)

    return command(conn, 'create', {'table': table_name, 'cols': cols, 'vals': vals})


def read(conn, table_name, filters):
    filters_str = sqlWhere(filters)
    return command(conn, 'read', {'table': table_name, 'filters': filters_str})


def update(conn, table_name, update_values, filters):
    update_str = sqlUpdateSet(update_values)
    filters_str = sqlWhere(filters)

    return command(conn, 'update', {'table': table_name, 'set_values': update_str, 'filters': filters_str})


def delete(conn, table_name, filters):
    filters_str = sqlWhere(filters)

    return command(conn, 'delete', {'table': table_name, 'filters': filters_str})


def loadCsv(conn, csv_path, table_name):

    if table_name is not None:
        with open(csv_path) as csv_file:
            rows = csv.reader(csv_file, delimiter=',', quotechar='"')           
            header = next(rows)

            # a = ['key1', 'key2', 'key3', 'key4', 'key5']
            # b = ['val1', 'val2', 'val3', 'val4', 'val5']

            for row in rows:
                record = dict(zip(header, row))
                create(conn, table_name, record)

                # tags = tag.tagRow(row)
                

            conn.commit()
            return True

        


