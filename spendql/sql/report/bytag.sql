SELECT  (CASE 
                WHEN tg.TAG_NAME IS NULL THEN "None"
                ELSE tg.TAG_NAME
        END) as TAG_NAME
        ,SUM(tr.AMMOUNT) as TOTAL
FROM    TRANSACT tr
        LEFT JOIN TRANSACT_M2M_TAG tr2tg ON tr2tg.TRANSACT_KEY = tr.TRANSACT_KEY
        LEFT JOIN TAGS tg                ON tg.TAG_KEY = tr2tg.TAG_KEY

GROUP BY tg.TAG_KEY