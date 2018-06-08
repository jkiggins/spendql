import os
import re
import csv

from . import spendql
from . import ctx

communityDatePat = re.compile('(\d\d\/\d\d\/)(\d\d)')

def communityPipe(rows):
    # CSV Headings: ACCOUNT, DATE_POSTED, SERIAL_NBR, DESCR, AMMOUNT, TYPE
    #                  0          1           2         3       4       5
    next(rows)  # Remove heading

    yield ('ACCOUNT','DATE_POSTED','DATE_TRANSACT','DESCR','AMMOUNT','CHECK_NUM')

    for row in rows:
        dtMatch = communityDatePat.search(row[3])
        date_transact = row[1]
        if dtMatch:
            date_transact = dtMatch.group(1) + '20{}'.format(dtMatch.group(2))

        yield (row[0], row[1], date_transact, row[3].upper(), row[4], int(row[2]))


def eslPipe(rows):
    pass

pipes = {'esl': eslPipe, 'com': communityPipe}

# ETL
def CSVRows(path):
    with open(path) as csv_file:
        rows = csv.reader(csv_file, delimiter=',', quotechar='"')

        for row in rows:
            yield row


def load(path, asType="esl"):
    if os.path.isfile(path) and asType in pipes:
        conn = spendql.getConn(ctx.db_name)
        rows = CSVRows(path)
        rows = pipes[asType](rows)

        spendql.createMany(conn, 'TRANSACT', next(rows), rows)

    else:
        print('Invalid Path: {0}'.format(path))