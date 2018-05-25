from . import spendql
from . import ctx
from . import router

# def noParams():
#     help(__module__)

def __setupDB(name):
    """Setups up a new spend database with a name of "name" if the database file already exists, it is deleted"""
    spendql.dropDB(name)
    conn = spendql.getConn(name)
    spendql.executeScriptFile(conn, 'setup')

def ctxCheck(next):
    if not spendql.dbExists(ctx.db_name):
        print('use a real database')
    else:
        next()


def use(name):
    if not spendql.dbExists(name):
        __setupDB(name)

    ctx.db_name = name


def reset():
    spendql.closeConn(name=ctx.db_name)
    __setupDB(ctx.db_name)


router.route('setup\.use', use)

router.route('setup.+', ctxCheck)
router.route('setup\.reset', reset)

