import os
import sqlite3
import csv
import re

fdir = os.path.dirname(__file__).replace('\\', '/')

rxNonAlphaNumeric = re.compile('[^a-z0-9_]+', re.I | re.M)
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
def strSanitize(dirty):
    search = rxNonAlphaNumeric.search(dirty)

    if search is None:
        return dirty


def sqlSanitizeDict(dirty):
    clean = {}
    for key in dirty:
        clean_key = sqlSanitize(key)
        clean_value = sqlSanitize(dirty[key])

        if (clean_key is None) or (clean_value is None):
            return None
        else:
            clean[clean_key] = clean_value


def sqlSanitizeList(dirty):
    dirty_test = ''.join(dirty)
    dirty_test = sqlSanitize(dirty_test)

    if dirty_test is not None:    
        return dirty


def sqlSanitize(dirty):
    if isinstance(dirty, dict):
        return sqlSanitizeDict(dirty)
    elif isinstance(dirty, str):
        return strSanitize(dirty)
    else:
        return sqlSanitizeList(dirty)


def sqlSanitizeAndJoin(dirty, join_str):
    clean = sqlSanitize(dirty)

    if clean is not None:
        return join_str.join(clean)


def readSqlFile(name):
    path = getSQLPath(name)
    with open(path, 'r') as sql:
        return sql.read()


def renderSql(sql, args):
    args = sqlSanitize(args)
    if args is not None:
        if isinstance(args, dict):
            return sql.format(**args)
        else:
            return sql.format(*args)


def renderSqlFile(name, args):
    sql = readSqlFile(name)
    return renderSql(sql, args)


def renderSqlUnsafe(sql, args):
    if isinstance(args, dict):
        return sql.format(**args)
    else:
        return sql.format(*args)


def renderSqlFileUnsafe(name, args):
    sql = readSqlFile(name)
    return renderSqlUnsafe(sql, args)


def renderKeyEqualsValue(mdict):
    render = ''
    for key, value in mdict.items():
        if(isinstance(value, str)):
            value = '\'' + value + '\''

        render += '{0}={1},\n'.format(key, value)


def renderFilters(filters):
    if isinstance(filters, str):
        return filters
    
    return renderKeyEqualsValue(filters)
        


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
def insertCsvIntoTable(conn, csv_path, table_name):

    if table_name is not None:
        with open(csv_path) as csv_file:
            rows = csv.reader(csv_file, delimiter=',')
            cursor = conn.cursor()

            cols = list(next(rows))
            
            params = {
                'table': table_name
                ,'cols': sqlSanitizeAndJoin(cols, ',')
                ,'vals': ','.join(['?'] * len(cols))
            }

            if params['cols'] is not None:
                sql = renderSqlFileUnsafe('insert', params)

                for row in rows:
                    cursor.execute(sql, row)

                conn.commit()
                return True


def dropTable(conn, table_name):
    return executeCommandFile(conn, 'drop', python_params=table_name)


def truncateTable(conn, table_name):
    return executeCommandFile(conn, 'truncate', python_params=table_name)


def dumpTable(conn, table_name):
    return executeCommandFile(conn, 'dump', python_params=table_name)

# record operations
def create(conn, table_name, record):
    cols = ','.join(list(record.keys()))
    vals = ','.join(list(record.values()))

    return executeCommandFile(conn, 'create', {'table': table_name, 'cols': cols, 'vals': vals})


def read(conn, table_name, filters):
    filters_str = renderFilters(filters)
    return executeCommandFile(conn, 'read', {'table': table_name, 'filters': filters_str})


def update(conn, table_name, update_values, filters):
    update_str = renderKeyEqualsValue(update_values)
    filters_str = filters
    
    if not isinstance(filters, str):
        filters_str = renderKeyEqualsValue(filters)

    return executeCommandFile(conn, 'update', {'table': table_name, 'set_values': update_str, 'filters': filters_str})


def delete(conn, table_name, filters):
    filters_str = renderFilters(filters)

    return executeCommandFile(conn, 'delete', {'table': table_name, 'filters': filters_str})

        


