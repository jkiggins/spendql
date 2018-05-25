INSERT INTO TRANSACT

SELECT      TRANSACT_KEY
            ,ACCOUNT
            ,DATE_POSTED
            ,SERIAL_NUMBER
            ,DESCRIPTION
            ,AMMOUNT
            ,TYPE 

FROM        TRANSACT_STAGE


INSERT INTO TRANSACT_M2M_TAG

SELECT      ts.TRANSACT_KEY
            ,tm.TAG_KEY

FROM        TRANSACT_STAGE  ts
            JOIN TAG_MATCH  tm  ON  (ts.ACCOUNT LIKE tm.MATCH_EXPR)
                                    AND (ts.AMMOUNT BETWEEN tm.MATCH_AMMOUNT_LOW AND tm.MATCH_AMMOUNT_HIGH)
