select *    
from warehouse.fact_raw_data
where asset in (ASSETS)
limit LIMIT
;