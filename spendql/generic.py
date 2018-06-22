from . import spendql
from . import display
from . import ctx

def exeAndPrint(path):
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.execute(conn, path)

    display.printCursor(cursor)