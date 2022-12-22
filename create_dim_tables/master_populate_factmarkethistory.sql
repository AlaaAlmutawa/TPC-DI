/*
---- Schema of FactMarketHistory table -> Refer Page 43 3.2.11
CREATE TABLE
  master.fact_market_history ( SK_SecurityID INT64 NOT NULL,
    --Surrogate key for SecurityID
    SK_CompanyID INT64 NOT NULL,
    -- Surrogate key for CompanyID
    SK_DateID INT64 NOT NULL,
    -- Surrogate key for the date

    PERatio NUMERIC,
    -- Price to earnings per share ratio
    Yield NUMERIC NOT NULL,
    -- Dividend to price ratio, as a percentage
    FiftyTwoWeekHigh NUMERIC NOT NULL,
    -- Security highest price in last 52 weeks from this day
    SK_FiftyTwoWeekHighDate INT64 NOT NULL,
    -- Earliest date on which the 52 week high price was set
    FiftyTwoWeekLow NUMERIC NOT NULL,
    -- Security lowest price in last 52 weeks from this day
    SK_FiftyTwoWeekLowDate INT64 NOT NULL,
    -- Earliest date on which the 52 week low price was set

    ClosePrice NUMERIC NOT NULL,
    -- Security closing price on this day
    DayHigh NUMERIC NOT NULL,
    -- Highest price for the security on this day
    DayLow NUMERIC NOT NULL,
    -- Lowest price for the security on this day
    Volume INT64 NOT NULL,
    -- Trading volume of the security on this day
    BatchID INT64 NOT NULL -- Batch ID when this record was inserted
    );
*/

-- insert into master.FactMarketHistory 


select
--   s.SK_SecurityID as SK_SecurityID,
--   s.SK_CompanyID as SK_CompanyID,
  d.SK_DateID as SK_DateID,
  dm.DM_CLOSE as ClosePrice, 
  dm.DM_HIGH as DayHigh, 
  dm.DM_Low as DayLow, 
  dm.DM_VOL as Volume,
  1 as BatchID
from staging.daily_market dm
join master.DimDate d on dm.DM_DATE = d.DateValue
-- join master.DimSecurity s on dm.DM_S_SYMB = s.Symbol and dm.DM_DATE >= s.EffectiveDate and dm.DM_DATE < s.EndDate
limit 10
;

/*MANO MANO

SELECT *, MAX(DM_HIGH) OVER(rolling_1yr_window) AS FiftyTwoWeekHigh
FROM staging.daily_market
where DM_S_SYMB='AAAAAAAAAAAABOY'
WINDOW rolling_1yr_window AS (    
  PARTITION BY DM_S_SYMB 
  ORDER BY DATE_DIFF(DM_DATE, '1970-01-01', YEAR)
  RANGE BETWEEN CURRENT ROW AND 0 FOLLOWING 
)
;
-------------------------------------------------------------------------

with merged as 
(select 
dm1.DM_S_SYMB,
dm1.DM_DATE as DM_DATE1, dm2.DM_DATE as DM_DATE2,
dm1.DM_HIGH as DM_HIGH1, dm2.DM_HIGH as DM_HIGH2
-- max(DM_HIGH) over (partition by DM_S_SYMB order by DM_DATE rows between 365 preceding and current row) FiftyTwoWeekHigh
from staging.daily_market dm1 
join staging.daily_market dm2 on dm1.DM_S_SYMB=dm2.DM_S_SYMB
where dm2.DM_DATE <= dm1.DM_DATE and dm2.DM_DATE> DATE_SUB(dm1.DM_DATE, INTERVAL 1 YEAR))

select * from
(
select *, row_number() over(partition by DM_S_SYMB, DM_DATE1 order by DM_HIGH desc) as rn
from merged
)a where rn=1;
;

-- TO ADD: If there are no earnings for this company, NULL is assigned to PERatio and an alert condition is raised as described below.
select CompanyName, Year, Quarter, QtrStartDate, sum(EPS) as EPS
from staging.finwire_fin_record 
where RecType='FIN'
group by CompanyName, Year, Quarter, QtrStartDate
limit 10;
-------------------------------------------------------------------------
-- TESTING

SELECT * -- max(DM_HIGH) as max_high, count(*) as cnt, max(DM_DATE)as max_date, min(DM_DATE) as min_date
FROM staging.daily_market
where DM_DATE>'2016-02-06' and DM_DATE<='2017-02-06' and DM_S_SYMB='AAAAAAAAAAAABOY';

-------------------------------------------------------------------------

with quarterly as (
  select
)

*/

