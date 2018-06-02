import sqlite3

from spendql import spendql
from spendql import setup
from spendql import transact
from spendql import tags


from spendql import display

def clean_db():
	setup.use('test')
	setup.reset()

def isEmpty(conn, table_name):
	cursor = conn.cursor()
	cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))

	return cursor.fetchone() is None

def isCursorEmpty(cursor):
	return cursor.fetchone() is None


# setup tests
def test_setup_reset():

	# TRANSACT STAGE TABLE
	# ---------------------
	# ACCOUNT TEXT
    # DATE_POSTED DATETIME
    # SERIAL_NUMBER INT
    # DESCRIPTION TEXT
    # AMMOUNT FLOAT
    # TYPE TEXT
	# ---------------------

	clean_db()
	
	conn = sqlite3.connect('spendql/dbs/test.db')
	conn.execute('INSERT INTO TRANSACT_STAGE (ACCOUNT, DATE_POSTED, SERIAL_NBR, DESCR, AMMOUNT, TYPE) VALUES ("test", "4/17/2017", 10, "test", 1.1, "test");')
	
	assert not isEmpty(conn, 'TRANSACT_STAGE'), "No records, insertion failed"

	# remove all items from db UUT
	setup.use('test')
	setup.reset()

	conn = sqlite3.connect('spendql/dbs/test.db')

	assert isEmpty(conn, 'TRANSACT_STAGE'), "found records, reset failed"


def testUseResetLoad():
	clean_db()

	# Open database and load csv
	setup.use(name='test')
	transact.load(path='./t.csv')

	# Check for records
	conn = spendql.getConn('test')
	cursor = spendql.dumpTable(conn, 'TRANSACT_STAGE')	
	assert cursor.fetchone() is not None, "Assert failed, table is empty"

	# Reset currently selected database (test)
	setup.reset()

	# Check that TRANSACT_STAGE is empty now
	conn = spendql.getConn('test')
	cursor = spendql.dumpTable(conn, 'TRANSACT_STAGE')
	assert cursor.fetchone() is None, "Assert failed, table is not empty"


def testTagAndMatchIO():
	clean_db()

	tags.addTag('test_tag', "This is a test tag")
	tags.addMatch('test_tag', '.+', 0, 100)

	conn = spendql.getConn('test')
	cursor = conn.cursor()

	result = cursor.execute('SELECT * FROM TAG_MATCH').fetchone()
	assert result is not None, "The match wasn't added"

	result = cursor.execute('SELECT * FROM TAGS').fetchone()
	assert result is not None, "The tag wasn't added"

	result = cursor.execute('SELECT * FROM TAGS t JOIN TAG_MATCH m ON m.TAG_KEY = t.TAG_KEY').fetchone()
	assert result is not None, "The Tag and match can't be joined"


def testGetTag():
	tags.addTag('test_tag2', "this is another test tag")
	tags.addTag('test_tag3', "yet another test tag")
	tags.getTags()


def testGetMatch():
	tags.addMatch('test_tag', 'regx', 0, 200)
	tags.addMatch('test_tag', 'xreg', 0, 500)
	tags.getMatch()


def testCRUD():
	# TAGS
	## COLUMNS ##
	# 	TAG_KEY INTEGER PRIMARY KEY
	# 	TAG_NAME TEXT
	# 	DESCR TEXT
	## CONSTRAINTS ##
	# 	UNIQUE(TAG_NAME)
	#
	clean_db()
	conn = spendql.getConn('test')

	spendql.create(conn, 'TAGS', {'TAG_NAME': 'test0', 'DESCR': 'DESCR0'})

	cursor = spendql.read(conn, 'TAGS', {'DESCR': 'DESCR0'})
	assert not isCursorEmpty(cursor), "read failed to return the record just added, dict"

	cursor = spendql.read(conn, 'TAGS', "DESCR='DESCR0'")
	assert not isCursorEmpty(cursor), "read failed to return the record just added, string"


	spendql.update(conn, 'TAGS', {'DESCR': 'DESCR1'}, {'DESCR': 'DESCR0'})
	cursor = spendql.read(conn, 'TAGS', {'DESCR': 'DESCR1'})
	# display.printCursor(cursor)
	assert not isCursorEmpty(cursor), "read failed to return the record just updated, dict"


	spendql.delete(conn, 'TAGS', {'DESCR': 'DESCR1'})
	cursor = spendql.read(conn, 'TAGS', {'DESCR': 'DESCR1'})
	assert isCursorEmpty(cursor), "failed to delete record, dict"


def testLoad():
	clean_db()

	# Open database and load csv
	setup.use('test')
	transact.load('./t.csv')

	# Check for records
	conn = spendql.getConn('test')
	cursor = spendql.dumpTable(conn, 'TRANSACT')	
	assert cursor.fetchone() is not None, "Assert failed, table is empty"


if __name__ == "__main__":
	# test_setup_reset()
	# test_setup_reset_router()
	# testUseResetLoad()

	# all or none sequence of tests
	# testTagAndMatchIO()
	# testGetTag()
	# testGetMatch()

	# testCRUD()

	testLoad()

	print('\n\ntest completed without error')



	
	
	
