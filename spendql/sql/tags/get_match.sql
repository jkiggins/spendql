SELECT      *
FROM        TAG_MATCH
WHERE       (
                :expr = '%'
                OR MATCH_EXPR LIKE :expr
            )
            AND (
                    :ammount = -1
                    -- OR :ammount BETWEEN MATCH_AMMOUNT_LOW AND MATCH_AMMOUNT_HIGH
                )
            AND (
                    :key_filter = -1
                    OR TAG_KEY = :key_filter
                )