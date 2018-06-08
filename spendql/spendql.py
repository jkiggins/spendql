import os
import sqlite3
import re

from . import ctx

fdir = os.path.dirname(__file__).replace('\\', '/')

rxNonAlphaNumeric = re.compile('[^a-z0-9_]+', re.I | re.M)
rxPassiveSql = re.compile('AND|OR|\=|<|>|=', re.I | re.M)
rxDoubleDot = re.compile('\.\.', re.I | re.M)

open_conns = {}
triggers = []

# Path Functions, abstract away the storage details
def getPath(suffix):
    if suffix[0] != '/':
        suffix = '/' + suffix

    return fdir + suffix


def getDBPath(name):
    return getPath('/dbs/{0}.db'.format(name))


def getSQLPath(name):
    name = name.replace('..', '')    
    name = name.replace('.', '/')
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

def getName(conn):
    tRecord = conn.execute('PRAGMA database_list;').fetchone()
    sName = tRecord[2].split('\\')[-1].split('.')[0]

    return sName

# Triggers
def trigger(apCall, arSQLPat):
    """When regex patterns dbname, table, and op match, call cb"""
    triggers.append({'call': apCall, 'sqlpat': arSQLPat})
   

def pullTriggers(asSQL):
    bCall = True
    for trig in triggers:
        if trig['sqlpat'].search(asSQL):
            trig['call']()


# raw sql functions
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


def renderSqlFile(asName, args):
    lsSql = readSqlFile(asName)
    lsSqlr = renderSql(lsSql, args)

    return lsSqlr
    


def sqlValue(val):        
    sVal = str(val)

    if val is None:
        sVal = 'NULL'

    return "'{}'".format(sVal)


def sqlValues(aiVals):
    vals = list(aiVals)

    sqlVals = [sqlValue(v) for v in vals]
    
    vals_str = '({})'.format(','.join(sqlVals))
    return vals_str


def sqlColumns(aiCols):
    cols = list(aiCols)
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


def command(conn, name, str_params=[]):
    cursor = conn.cursor()

    lsSqlr = renderSqlFile(name, str_params)

    if ctx.DEBUG_SQL:
        print(lsSqlr)
    else:
        cursor.execute(lsSqlr)
        conn.commit()
    
    pullTriggers(lsSqlr)        
    return cursor


def script(conn, name, str_params=[]):
    cursor = conn.cursor()

    lsSqlr = renderSqlFile(name, str_params)

    if ctx.DEBUG_SQL:
        print(lsSqlr)
    else:
        cursor.executescript(lsSqlr)
        conn.commit()

    pullTriggers(lsSqlr)
    return cursor


# Table operations
def dropTable(conn, table_name):
    return command(conn, 'drop', str_params=table_name)


def truncateTable(conn, table_name):
    return command(conn, 'truncate', str_params=table_name)


def dumpTable(conn, table_name):
    return command(conn, 'dump', str_params=table_name)


# record operations
def createOne(conn, table_name, record):
    cols = sqlColumns(record)
    vals = sqlValues(record.values())

    return command(conn, 'create', {'table': table_name, 'cols': cols, 'vals': vals})


def createMany(conn, table_name, cols, records):

    llVals = [sqlValues(r) for r in records]
    lsVals = ',\n'.join(llVals)

    lsCols = sqlColumns(cols)

    return script(conn, 'create', {'table': table_name, 'cols': lsCols, 'vals': lsVals})


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


        


