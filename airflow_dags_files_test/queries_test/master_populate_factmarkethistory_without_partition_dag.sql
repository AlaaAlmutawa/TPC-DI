--4.5.11 FactMarketHistory pg 67
--insert into master.FactMarketHistory 
with daily_market_dates as (
    select 
    dm.DM_DATE
    , dm.DM_S_SYMB
    , dm.DM_CLOSE
    , dm.DM_HIGH
    , dm.DM_LOW
    , dm.DM_VOL
    , dd.SK_DateID
    from staging1.daily_market dm
    inner join master1.DimDate dd on dm.DM_DATE = dd.DateValue
)
, daily_market_with_past52_weeks as (
    select 
    r.DM_DATE
    , r.DM_S_SYMB
    , r.DM_CLOSE
    , r.DM_HIGH
    , r.DM_LOW
    , r.DM_VOL
    , r.SK_DateID
    , l.DM_DATE as DM_DATE_LEFT
    , l.DM_HIGH as DM_HIGH_LEFT 
    , l.DM_LOW as DM_LOW_LEFT
    , l.SK_DateID as SK_DateID_LEFT
    from daily_market_dates r
    join daily_market_dates l 
    on r.DM_DATE <= l.DM_DATE --check its less (upper boundry) 
    and r.DM_S_SYMB = l.DM_S_SYMB -- check for symbol
    and r.DM_DATE > DATE_SUB(l.DM_DATE, INTERVAL 1 YEAR) --check if its bigger than l.dimdate - 1 year (lower boundry )
)
, agg1_highest as (
    select *
    ,row_number() over(partition by DM_DATE, DM_S_SYMB order by DM_HIGH_LEFT DESC, DM_DATE_LEFT ASC) as rank
    from daily_market_with_past52_weeks
)
, agg2_lowest as (
    select *
    ,row_number() over(partition by DM_DATE, DM_S_SYMB order by DM_HIGH_LEFT ASC, DM_DATE_LEFT ASC) as rank
    from daily_market_with_past52_weeks
)
, get_highest as (
    select 
    DM_DATE
    , DM_S_SYMB
    , DM_CLOSE
    , DM_HIGH
    , DM_LOW
    , DM_VOL
    , SK_DateID 
    , DM_DATE_LEFT as highest_date
    , DM_HIGH_LEFT as highest
    , SK_DateID_LEFT as highest_SK_DateID 
    from agg1_highest 
    where rank = 1
)
, get_lowest as (
    select 
    DM_DATE
    , DM_S_SYMB
    , DM_CLOSE
    , DM_HIGH
    , DM_LOW
    , DM_VOL
    , SK_DateID 
    , DM_DATE_LEFT as low_date 
    , DM_LOW_LEFT as lowest
    , SK_DateID_LEFT as lowest_SK_DateID
    from agg2_lowest
    where rank = 1
)
, get_dm_details as (
    select 
    gh.DM_DATE
    , gh.DM_S_SYMB
    , gh.DM_CLOSE
    , gh.DM_HIGH
    , gh.DM_LOW
    , gh.DM_VOL
    , gh.SK_DateID 
    , highest as FiftyTwoWeekHigh
    , highest_SK_DateID as SK_FiftyTwoWeekHighDate
    , lowest as FiftyTwoWeekLow
    , lowest_SK_DateID as SK_FiftyTwoWeekLowDate
    from get_highest gh
    join get_lowest gl on gh.DM_DATE = gl.DM_DATE AND gh.DM_S_SYMB = gl.DM_S_SYMB AND gh.DM_CLOSE = gl.DM_CLOSE AND gh.DM_HIGH = gl.DM_HIGH 
    AND gh.DM_LOW = gl.DM_LOW AND gh.DM_VOL = gl.DM_VOL AND gh.SK_DateID = gl.SK_DateID
)
, get_quarters as (
    select
    f.SK_CompanyID
    ,f.FI_QTR_START_DATE
    , sum(FI_BASIC_EPS) over (partition by c.CompanyID order by f.FI_QTR_START_DATE rows between 3 preceding and current row ) AS eps_qtr_sum
    , lead(FI_QTR_START_DATE, 1, DATE('9999-12-31')) over (partition by c.CompanyID order by f.FI_QTR_START_DATE asc) AS next_qtr_start
    from master1.Financial f 
    join master1.DimCompany c
    on f.SK_CompanyID = c.SK_CompanyID
)

SELECT 
SK_SecurityID
, sec.SK_CompanyID
, SK_DateID 
, if(gq.eps_qtr_sum != 0 and gq.eps_qtr_sum is not null, round(cast((dm.DM_CLOSE / gq.eps_qtr_sum) as NUMERIC),2), null) as PERatio
, if(sec.Dividend != 0, round(cast((dm.DM_CLOSE / sec.Dividend) as NUMERIC),2), 0) as Yield
, round(cast(FiftyTwoWeekHigh as NUMERIC),2) as FiftyTwoWeekHigh
, SK_FiftyTwoWeekHighDate
, round(cast(FiftyTwoWeekLow as NUMERIC),2) as FiftyTwoWeekLow
, SK_FiftyTwoWeekLowDate
, cast(DM_CLOSE as NUMERIC) AS ClosePrice
, cast(DM_HIGH as NUMERIC) AS DayHigh
, cast(DM_LOW as NUMERIC) AS DayLow
, DM_VOL AS Volume
, 1 AS BatchID 
FROM get_dm_details dm 
JOIN master1.DimSecurity sec 
ON dm.DM_S_SYMB = sec.Symbol AND dm.DM_DATE >= sec.EffectiveDate and dm.DM_DATE < sec.EndDate
JOIN get_quarters gq on sec.SK_CompanyID = gq.SK_CompanyID and gq.FI_QTR_START_DATE <= dm.DM_DATE and gq.next_qtr_start > dm.DM_DATE