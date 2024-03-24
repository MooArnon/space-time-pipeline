SELECT 
    *
FROM aggregated.classifier_voted_evaluation
WHERE scraped_timestamp > '2024-03-11 00:00:00'
    AND scraped_timestamp < '2024-03-12 00:00:00'
;
