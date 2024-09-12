-- CTE to calculate price differences
-- Keeps price differences of each time steps
WITH price_diff AS (
    SELECT
        id,
        price,
        asset,
        lag(price, 1) over(
            order by scraped_timestamp desc
        ) - price as diff,
        scraped_timestamp
    FROM fact_raw_data
    WHERE asset = '<ASSET>'  
        AND scraped_date >= date_add('day', -<NUMBER_LOOK_BACK_DATE>, current_date)
    ORDER BY scraped_timestamp desc
    LIMIT <EVALUATION_RANGE>
),

-- CTE to generate true results based on price differences
true_result AS (
    SELECT
        *,
        CASE
            WHEN diff > 0 THEN 1
            WHEN diff < 0 THEN 0
        END AS true_result
    FROM price_diff
),
model_prediction AS (
    SELECT 
        asset
        , raw_data_id
        , model_type
        , prediction
        , predicted_timestamp
    FROM fact_classifier_prediction as pred
    WHERE asset = '<ASSET>' 
        AND predicted_date >= date_add('day', -<NUMBER_LOOK_BACK_DATE>, current_date)
        AND EXISTS (
            SELECT 1
            FROM true_result AS actual
            WHERE pred.raw_data_id = actual.id
        )
),
true_result_vs_pred AS (
    SELECT
        actual.id,
        actual.true_result,
        actual.scraped_timestamp,
        pred.model_type,
        actual.asset,
        CAST(ROUND(pred.prediction) AS INTEGER) AS prediction,
        CASE
            WHEN actual.true_result = ROUND(pred.prediction) THEN 'correct'
            ELSE 'wrong'
        END AS result,
        CASE
            WHEN ROUND(pred.prediction) = 0 THEN 'sell'
            WHEN ROUND(pred.prediction) = 1 THEN 'buy'
        END AS signal
    FROM true_result AS actual
    LEFT JOIN model_prediction AS pred ON actual.id = pred.raw_data_id
    WHERE prediction IS NOT NULL
    AND true_result IS NOT NULL
),
total_predict_per_model AS (
    SELECT
        model_type,
        asset,
        COUNT(1) AS total_predicted
    FROM true_result_vs_pred
    GROUP BY model_type, asset
),
corrected_predictions AS (
    SELECT 
        count(1) as corrected_prediction
        , model_type
        , asset
        , result
    FROM true_result_vs_pred
    WHERE result = 'correct'
    GROUP BY model_type, asset, result
),
accuracy AS (
    SELECT
        pred.result
        , pred.model_type
        , total.asset
        , corrected_prediction
        , total.total_predicted
        , (CAST(corrected_prediction AS DOUBLE) / CAST(total.total_predicted AS DOUBLE)) AS accuracy
    FROM corrected_predictions AS pred
    LEFT JOIN total_predict_per_model AS total ON pred.model_type = total.model_type
),

accuracy_sum AS (
    SELECT
        SUM(accuracy) AS total_accuracy
    FROM accuracy
),
-- Calculate model weights based on their accuracy
model_weights AS (
    SELECT
        a.model_type,
        a.asset,
        a.corrected_prediction,
        a.total_predicted,
        a.accuracy,
        (a.accuracy / s.total_accuracy) AS weight -- Weight based on accuracy
    FROM accuracy AS a
    CROSS JOIN accuracy_sum AS s
)

SELECT
    model_type
    , asset
    , corrected_prediction
    , total_predicted
    , accuracy
    , weight
FROM model_weights;
