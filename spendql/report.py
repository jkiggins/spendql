from . import generic
from . import spendql
from . import ctx
from . import display

def all():
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.dumpTable(conn, 'TRANSACT')

    display.printCursor(cursor)


def byTag():
    generic.exeAndPrint('report.bytag')


def untagged():
    generic.exeAndPrint('report.untagged')


