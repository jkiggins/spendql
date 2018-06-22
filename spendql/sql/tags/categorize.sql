INSERT INTO TRANSACT_M2M_TAG (TRANSACT_KEY, TAG_KEY)

SELECT      TRANSACT.TRANSACT_KEY, t.TAG_KEY
FROM        TRANSACT

            JOIN             TAGS t
            LEFT OUTER JOIN  TRANSACT_M2M_TAG tn ON  tn.TAG_KEY = t.TAG_KEY AND tn.TRANSACT_KEY = TRANSACT.TRANSACT_KEY

WHERE       (
                (
                    instr(TRANSACT.DESCR, 'WEGMANS') > 0
                    AND (t.TAG_NAME = 'WEGMANS' or t.TAG_NAME = 'GROCERY')
                )
                OR(
                    instr(TRANSACT.DESCR, 'WALMART') > 0
                    AND t.TAG_NAME = 'WALMART'
                )
                OR (
                    instr(TRANSACT.DESCR, 'AMAZON') > 0
                    AND t.TAG_NAME = 'AMAZON'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'COSTCO WHSE') > 0
                    )
                    AND t.TAG_NAME = 'GROCERY'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'FIVE GUYS') > 0
                        OR instr(TRANSACT.DESCR, 'DENNYS') > 0
                        OR instr(TRANSACT.DESCR, 'TACO BELL') > 0
                        OR instr(TRANSACT.DESCR, 'RIT CAFE') > 0
                        OR instr(TRANSACT.DESCR, 'SUBWAY') > 0
                        OR instr(TRANSACT.DESCR, 'HONEOYE FALLS MARK') > 0
                        OR instr(TRANSACT.DESCR, 'CRITIC S FAMILY') > 0
                        OR instr(TRANSACT.DESCR, 'FOODSERVICE') > 0
                        OR instr(TRANSACT.DESCR, 'AMIELS THE ORGINAL SUB') > 0
                        OR instr(TRANSACT.DESCR, 'CHEESECAKE ROCHESTER') > 0
                        OR instr(TRANSACT.DESCR, 'PICRAFT ROCHESTER') > 0
                        OR instr(TRANSACT.DESCR, 'RITBYTES') > 0
                        OR instr(TRANSACT.DESCR, 'CTRL ALT DELI') > 0
                        OR instr(TRANSACT.DESCR, 'JAVAS AT RIT') > 0
                        OR instr(TRANSACT.DESCR, 'INSOMNIA COOKIES') > 0
                        OR instr(TRANSACT.DESCR, 'BAR EATO GENESEO') > 0
                        OR instr(TRANSACT.DESCR, 'COLDSTONE') > 0
                        OR instr(TRANSACT.DESCR, 'STRONG HEARTS') > 0
                        OR instr(TRANSACT.DESCR, ' RIT ') > 0
                        OR instr(TRANSACT.DESCR, ' TULLYS ') > 0

                    )
                    AND t.TAG_NAME = 'RESTAURANT'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'EXXONMOBIL') > 0
                        OR instr(TRANSACT.DESCR, 'SUNOCO') > 0
                        OR instr(TRANSACT.DESCR, 'KWIK') > 0
                        OR instr(TRANSACT.DESCR, 'COSTCO GAS') > 0
                        OR instr(TRANSACT.DESCR, 'BJS FUEL') > 0
                        OR instr(TRANSACT.DESCR, 'HYLAN DR FAST') > 0
                        OR instr(TRANSACT.DESCR, 'CICERO FASTRA') > 0
                        OR instr(TRANSACT.DESCR, '7ELEVEN') > 0
                    )
                    AND t.TAG_NAME = 'GAS'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'AUTOZONE') > 0
                        OR instr(TRANSACT.DESCR, 'DELTA SONIC CW') > 0
                    )
                    AND t.TAG_NAME = 'CAR'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'SEARS') > 0
                        OR instr(TRANSACT.DESCR, 'JCPENNEY') > 0
                        OR instr(TRANSACT.DESCR, 'SIERRA TRADING POST') > 0
                    )
                    AND t.TAG_NAME = 'CLOTHES'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'MOOG') > 0
                        OR instr(TRANSACT.DESCR, 'QUICKBOOKS') > 0
                    )
                    AND t.TAG_NAME = 'PAY'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'INTUIT TURBOTAX') > 0
                    )
                    AND t.TAG_NAME = 'FINANCIAL_SERVICES'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'SUPERCUTS') > 0
                    )
                    AND t.TAG_NAME = 'HEALTH'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'OLLIES BARGAIN OUTLET') > 0
                        OR instr(TRANSACT.DESCR, 'OPSRUSTIC') > 0
                        OR instr(TRANSACT.DESCR, 'BED BATH') > 0                        
                    )                
                    AND t.TAG_NAME = 'HOME'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'JOANN STORES') > 0
                        OR instr(TRANSACT.DESCR, 'THE HOME DEPOT') > 0                        
                    )                
                    AND t.TAG_NAME = 'PROJECTS'
                )
                OR (
                    CHECK_NUM != 0
                    AND TRANSACT.AMMOUNT = -700                
                    AND t.TAG_NAME = 'RENT'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'U OF R') > 0
                        OR instr(TRANSACT.DESCR, 'STRONG MEMORIAL') > 0
                        OR instr(TRANSACT.DESCR, 'VICTOR ADVANCED CHIROP') > 0
                        OR instr(TRANSACT.DESCR, 'LATTIMORE OF RUSH') > 0                        
                    )                
                    AND t.TAG_NAME = 'MEDICAL'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'ATM WITHDRAWAL') > 0
                    )                
                    AND t.TAG_NAME = 'CASH'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'DAVE & BUSTERS') > 0
                        OR instr(TRANSACT.DESCR, 'SPOTIFY') > 0
                        OR instr(TRANSACT.DESCR, 'IMAGINE DRAGONS') > 0
                        OR instr(TRANSACT.DESCR, 'RPM RACEWAY') > 0
                        OR instr(TRANSACT.DESCR, 'CITY OF ROCHPARK') > 0
                        OR instr(TRANSACT.DESCR, 'THE FIRING PIN') > 0
                    )
                    AND t.TAG_NAME = 'ENTERTAIN'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'POWERHOUSE GYM') > 0
                        OR instr(TRANSACT.DESCR, '214 ERIE AND GEDD') > 0
                        OR instr(TRANSACT.DESCR, ' DICKS ') > 0
                    )
                    AND t.TAG_NAME = 'SPORT'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, 'COMPANION ANIMAL HOSP') > 0
                    )
                    AND t.TAG_NAME = 'KITTY'
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, ' SKI ') > 0
                        OR instr(TRANSACT.DESCR, 'SKI BRISTOL') > 0
                        OR (instr(TRANSACT.DESCR, 'ATM WITHDRAWAL') AND TRANSACT.AMMOUNT = 80)
                    )
                    AND (t.TAG_NAME = 'SKIING' OR t.TAG_NAME = 'SPORT')
                )
                OR (
                    (
                        instr(TRANSACT.DESCR, ' SAV') > 0
                        OR instr(TRANSACT.DESCR, '1376479075 CK') > 0
                        OR instr(TRANSACT.DESCR,  'PAYMENT  THANK YOU')
                        OR instr(TRANSACT.DESCR,  'DEPOSIT RENT')
                        OR instr(TRANSACT.DESCR,  'ACH DEPOSIT JACOB')
                        OR instr(TRANSACT.DESCR,  'ACH DEPOSIT FIRST')
                        OR TRANSACT.ACCOUNT = 'ESL SAVINGS'
                    )
                    AND t.TAG_NAME = 'NET-ZERO'
                )
            )
            AND TRANSACT.DIRTY = 1
            AND tn.TAG_KEY is NULL;

