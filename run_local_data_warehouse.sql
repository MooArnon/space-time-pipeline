-- transform stg
DROP TABLE IF EXISTS tmp_fact_raw_data;
CREATE TEMP TABLE tmp_fact_raw_data AS
SELECT 
    asset,
    scraped_timestamp,
    CAST(price AS DECIMAL(21, 10)) AS price,
    source_id,
    engine_id,
    CURRENT_TIMESTAMP AS created_timestamp,
    CURRENT_TIMESTAMP AS updated_timestamp,
    TO_CHAR(scraped_timestamp, 'YYYY-MM-DD HH24:00:00') AS scraped_timestamp_hour
FROM staging.fact_raw_data;

-- insert new record
INSERT INTO warehouse.fact_raw_data (
    asset,
    scraped_timestamp,
    price,
    source_id,
    engine_id,
    created_timestamp,
    updated_timestamp
)
SELECT 
    tmp.asset,
    tmp.scraped_timestamp,
    tmp.price,
    tmp.source_id,
    tmp.engine_id,
    tmp.created_timestamp,
    tmp.updated_timestamp
FROM tmp_fact_raw_data AS tmp
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.fact_raw_data AS dest
    WHERE dest.asset = tmp.asset
    AND dest.scraped_timestamp = tmp.scraped_timestamp
    AND dest.source_id = tmp.source_id
);

-- update
UPDATE warehouse.fact_raw_data AS dest
SET 
    price = tmp.price,
    engine_id = tmp.engine_id,
    updated_timestamp = tmp.updated_timestamp
FROM tmp_fact_raw_data AS tmp 
WHERE dest.asset = tmp.asset
    AND dest.scraped_timestamp = tmp.scraped_timestamp
    AND dest.source_id = tmp.source_id;

TRUNCATE TABLE staging.fact_raw_data;
COMMIT;
