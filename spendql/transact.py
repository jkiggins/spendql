import os
from . import spendql
from . import ctx

def load(path):
    if os.path.isfile(path):
        conn = spendql.getConn(ctx.db_name)
        spendql.loadCsv(conn, path, 'TRANSACT')
    else:
        print('Invalid Path: {0}'.format(path))