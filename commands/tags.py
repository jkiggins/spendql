from . import spendql
from . import ctx
from . import router
from . import display

def __buildTagsFromMatch(match_id):
    conn = spendql.getConn(ctx.db_name)
    spendql.executeScriptFile(conn, 'build_tags', python_params=match_id)

def ctxCheck(next):
    if not spendql.dbExists(ctx.db_name):
        print('use a real database')
    else:
        next()

def getTags(name='%', descr='%'):
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.executeCommandFile(conn, 'get_tags', sql_params={'name_filter': name, 'descr_filter': descr})
    display.printCursor(cursor)


def addTag(name, descr):
    conn = spendql.getConn(ctx.db_name)
    spendql.insertIntoTable(conn, 'TAGS', {'TAG_NAME': name, 'DESCR': descr})


def updateTag(key=-1, name_filter='%', descr_filter='%', name='', descr=''):
    pass


def getMatch(expr='%', ammount=-1):
    conn = spendql.getConn(ctx.db_name)
    cursor = spendql.executeCommandFile(conn, 'get_match', sql_params={'expr': expr, 'ammount': ammount})
    display.printCursor(cursor)


def addMatch(tag_name, expr, low, high):
    conn = spendql.getConn(ctx.db_name)
    params = {'tag_name': tag_name, 'expr': expr, 'low': low, 'high': high}
    spendql.executeCommandFile(conn, 'add_match_to_tag', sql_params=params)



router.route('tags\..*', ctxCheck)
router.route('tags', getTags)
router.route('tags.add', addTag)
router.route('tags.match', getMatch)
router.route('tags.match.add', addMatch)


