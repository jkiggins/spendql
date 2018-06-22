import os
import re
import csv
import hashlib

from . import spendql
from . import ctx

communityDatePat = re.compile('(\d\d\/\d\d\/)(\d\d)')

def stipSpecial(aStr):
    nonAlphaNum = re.compile('[^A-Za-z0-9\s]')
    return nonAlphaNum.sub('', aStr)

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
        
        amnt = row[4]
        if row[5] == 'DR':
            amnt = -row[4]

        yield (row[0], row[1], date_transact, row[3], amnt, int(row[2]))


def eslPipe(rows):
    # CSV HEADINGS: Transaction Number, Date, Description, Memo, Debit, Credit, Balance, Check Number, Fees
    #                       0             1         2        3     4       5       6           7        8

    acctTypeRow = next(rows)
    acctType = 'ESL'

    if 'Checking' in acctTypeRow[0]:
        acctType = 'ESL Checking'
    elif 'Dividend' in acctTypeRow[0]:
        acctType = 'ESL Savings'
    elif 'Visa' in acctTypeRow[0]:
        acctType = 'ESL Secured CC'


    while(next(rows)[0] != "Transaction Number"):
        # Ignore heading and metadata
        pass
    
    yield ('TRANSACT_HASH','ACCOUNT','DATE_POSTED','DATE_TRANSACT','DESCR','AMMOUNT','CHECK_NUM')
    
    for row in rows:
        credit = 0 if row[5] == '' else float(row[5])
        debit = 0 if row[4] == '' else float(row[4])
        chkNum = 0 if row[7] == '' else int(row[7])
        amnt = credit + debit

        descr = stipSpecial(row[2]) + ' ' + stipSpecial(row[3])
        recordHash = hashlib.md5(bytes('|'.join(row), encoding='utf-8')).hexdigest()

        yield (recordHash, acctType, row[1], row[1], descr, amnt, chkNum)


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

        spendql.createMany(conn, 'TRANSACT', next(rows), rows, ignoreDuplicates=True)

    else:
        print('Invalid Path: {0}'.format(path))