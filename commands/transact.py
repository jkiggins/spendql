import os
from . import spendql
from . import ctx
from . import router

def load(path):
    if os.path.isfile(path):
        conn = spendql.getConn(ctx.db_name)
        spendql.insertCsvIntoTable(conn, path, 'TRANSACT_STAGE')
    else:
        print('Invalid Path: {0}'.format(path))


router.route('transact.load', load)