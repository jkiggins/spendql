SELECT      *
FROM        TAGS
WHERE       (
                :name_filter = '%'
                OR TAG_NAME LIKE :name_filter
            )
            AND (
                    :descr_filter = '%'
                    OR DESCR LIKE :descr_filter
                )