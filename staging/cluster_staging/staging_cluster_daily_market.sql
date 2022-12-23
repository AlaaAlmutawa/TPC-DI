CREATE TABLE `staging.daily_market_v2`
PARTITION BY DM_DATE
CLUSTER BY DM_S_SYMB
OPTIONS(
   description="Trying to improve tranformations. for FactMarketHistory"
) AS select * from staging.daily_market