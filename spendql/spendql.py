import os
import sqlite3
import csv
import re
import itertools

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
def strSanitize(dirty, mode=''):
    test = dirty
    if mode.upper() == 'WHERE':
        test = rxPassiveSql.sub('', test)

    search = rxNonAlphaNumeric.search(test)

    if search is None:
        return dirty


def sqlSanitizeDict(dirty, mode=''):
    for key in dirty:
        clean_key = sqlSanitize(key, mode=mode)
        clean_value = sqlSanitize(dirty[key], mode=mode)

        if (clean_key is None) or (clean_value is None):
            return None

    return dirty


def sqlSanitizeList(dirty, mode=''):
    dirty_test = ''.join(dirty)
    dirty_test = sqlSanitize(dirty_test, mode=mode)

    if dirty_test is not None:    
        return dirty


def sqlSanitize(dirty, mode=''):
    if isinstance(dirty, dict):
        return sqlSanitizeDict(dirty, mode=mode)
    elif isinstance(dirty, str):
        return strSanitize(dirty, mode=mode)
    elif len(dirty) > 0:
        return sqlSanitizeList(dirty, mode=mode)


def readSqlFile(name):
    path = getSQLPath(name)
    with open(path, 'r') as sql:
        return sql.read()


def renderSql(sql, args):
    if isinstance(args, dict):
        return sql.format(**args)
    else:
        return sql.format(*args)


def renderSqlFile(name, args):
    sql = readSqlFile(name)
    return renderSql(sql, args)


def sqlValue(val):
    if not(isinstance(val, int) or isinstance(val, float)):
        val = "'{}'".format(val)
    return val


def sqlWhere(filters):
    where_clause = ''
    if isinstance(filters, dict):
        if sqlSanitize(filters) is not None:
            key_value = ['{}={}'.format(key, sqlValue(filters[key])) for key in filters]
            where_clause += ' AND '.join(key_value)

    elif isinstance(filters, str):
        if sqlSanitize(filters, mode='WHERE') is not None:
            where_clause += filters

    if where_clause != '':
        where_clause = 'WHERE ' + where_clause
    
    return where_clause


def sqlUpdateSet(values):
    if sqlSanitize(values) is not None:
        key_value = ['{}={}'.format(key, sqlValue(values[key])) for key in values]
        return 'SET ' + ','.join(key_value)


def sqlColumns(col_val):
    cols = list(col_val.keys())

    if sqlSanitize(cols) is not None:
        return '({})'.format(','.join(cols))


def sqlValues(col_val):
    vals = list(col_val.values())

    if sqlSanitize(vals) is not None:
        vals_sql = [sqlValue(val) for val in vals]
        vals_str = ','.join(vals_sql)
        return 'VALUES ({})'.format(vals_str)


# Execute sql files as a command or script, multiple statments (delimited by ;) makes a script
# a single statment makes a command
def executeCommandFile(conn, name, python_params=[], sql_params=[]):
    cursor = conn.cursor()

    sql = renderSqlFile(name, python_params)

    if sql is not None:
        cursor.execute(sql, sql_params)
        conn.commit()
        return cursor


def executeScriptFile(conn, name, python_params=[]):
    cursor = conn.cursor()

    sql = renderSqlFile(name, python_params)

    if sql is not None:
        cursor.executescript(sql)
        conn.commit()
        return cursor


# Table operations

def dropTable(conn, table_name):
    return executeCommandFile(conn, 'drop', python_params=table_name)


def truncateTable(conn, table_name):
    return executeCommandFile(conn, 'truncate', python_params=table_name)


def dumpTable(conn, table_name):
    return executeCommandFile(conn, 'dump', python_params=table_name)

# record operations
def create(conn, table_name, record):
    cols = sqlColumns(record)
    vals = sqlValues(record)

    return executeCommandFile(conn, 'create', {'table': table_name, 'cols': cols, 'vals': vals})


def read(conn, table_name, filters):
    filters_str = sqlWhere(filters)
    return executeCommandFile(conn, 'read', {'table': table_name, 'filters': filters_str})


def update(conn, table_name, update_values, filters):
    update_str = sqlUpdateSet(update_values)
    filters_str = sqlWhere(filters)

    return executeCommandFile(conn, 'update', {'table': table_name, 'set_values': update_str, 'filters': filters_str})


def delete(conn, table_name, filters):
    filters_str = sqlWhere(filters)

    return executeCommandFile(conn, 'delete', {'table': table_name, 'filters': filters_str})


def insertCsvIntoTable(conn, csv_path, table_name):

    if table_name is not None:
        with open(csv_path) as csv_file:            
            header = next(rows)

            for row in rows:
                record = dict(itertools.product(header, row))
                create(conn, table_name, record)

            conn.commit()
            return True

        


