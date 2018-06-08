from . import ctx
from . import spendql
from . import display

def lst():
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.dumpTable(conn, 'TAGS')

    display.printCursor(cursor)

def tagDirty():
    tag()

def tag(str_params=None):
    conn = spendql.getConn(ctx.db_name)
    spendql.script(conn, 'tags.categorize', str_params=str_params)

