UPDATE TAGS
SET TAG_NAME = (CASE WHEN :name = '' THEN TAG_NAME ELSE :name)
SET DESCR = (CASE WHEN :name = '' THEN TAG_NAME ELSE :name) 
WHERE       (
                :name_filter = '%'
                OR TAG_NAME LIKE :name_filter
            )
            AND (
                    :descr_filter = '%'
                    OR DESCR LIKE :descr_filter
                )