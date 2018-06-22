INSERT INTO TRANSACT_M2M_TAG (TRANSACT_KEY, TAG_KEY)

SELECT      TRANSACT.TRANSACT_KEY, t.TAG_KEY
FROM        TRANSACT

            JOIN             TAGS t
            LEFT OUTER JOIN  TRANSACT_M2M_TAG tn ON  tn.TAG_KEY = t.TAG_KEY AND tn.TRANSACT_KEY = TRANSACT.TRANSACT_KEY

WHERE       (
                (
                    instr(TRANSACT.DESCR, 'WEGMANS') > 0
                    AND UPPER(t.TAG_NAME) = 'WEGMANS'
                )
                OR (
                    instr(TRANSACT.DESCR, 'AMAZON') > 0
                    AND UPPER(t.TAG_NAME) = 'AMAZON'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'EXXONMOBIL') > 0
                        OR instr(TRANSACT.DESCR, 'SUNOCO') > 0
                    )
                    AND t.TAG_NAME = 'GAS'
                )
                OR (
                    instr(TRANSACT.DESCR, 'AUTOZONE') > 0
                    AND t.TAG_NAME = 'CAR'
                )
                OR (
                    TRANSACT.AMMOUNT > 0
                    AND t.TAG_NAME = 'PAY'
                )
                OR (
                    instr(TRANSACT.DESCR, 'OLLIES BARGAIN OUTLET') > 0
                    AND t.TAG_NAME = 'HOME'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'DAVE & BUSTERS') > 0
                        OR instr(TRANSACT.DESCR, 'SPOTIFY') > 0
                    )
                    AND t.TAG_NAME = 'ENTERTAIN'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, ' SAV') > 0
                    )
                    AND t.TAG_NAME = 'NET-ZERO'
                )
            )
            AND TRANSACT.DIRTY = 1
            AND tn.TAG_KEY is NULL;
