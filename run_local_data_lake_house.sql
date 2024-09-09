SELECT 
    CAST(scraped_timestamp AS TIMESTAMP(3)) AS scraped_timestamp
    , price
FROM fact_raw_data
WHERE asset = '<ASSET>'
    AND scraped_timestamp > CAST('<LOWER_TIMESTAMP_BOUNDARY>' AS TIMESTAMP(3))
ORDER BY scraped_timestamp ASC
LIMIT <LIMIT>
;
