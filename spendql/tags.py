from . import ctx
from . import spendql
from . import display

def lst():
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.dumpTable(conn, 'TAGS')

    display.printCursor(cursor)


def add(tagName):
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.createOne(conn, "TAGS", {'TAG_NAME': tagName})

    display.printCursor(cursor)


def remove(tagName):
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.delete(conn, 'TAGS', {'TAG_NAME': tagName.upper()})

    display.printCursor(cursor)


def tagDirty():
    conn = spendql.getConn(ctx.db_name)
    spendql.script(conn, 'tags.categorize')
    spendql.script(conn, 'tags.credit_untagged')
    spendql.script(conn, 'transact.cleanall')


def tagUntagged():
    conn = spendql.getConn(ctx.db_name)
    spendql.script(conn, 'transact.dirtyuntaged')
    spendql.script(conn, 'tags.categorize')


def reTag():
    conn = spendql.getConn(ctx.db_name)
    spendql.script(conn, 'tags.dirtyall')

    tagDirty()


