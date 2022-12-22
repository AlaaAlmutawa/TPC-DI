/*
-- master schema
CREATE TABLE master.FactWatches (
    SK_CustomerID INT64 NOT NULL,
    SK_SecurityID INT64 NOT NULL,
    SK_DateID_DatePlaced INT64 NOT NULL,
    SK_DateID_DateRemoved INT64,
    BatchID INT64 NOT NULL
);
*/

with 
watch_hist as
  (
    select
        cust.SK_CustomerID as SK_CustomerID,
        s.SK_SecurityID as SK_SecurityID,
        d.SK_DateID as SK_DateID,
        W_ACTION
    from staging.watch_history wh
    join master.DimCustomer cust on wh.W_C_ID=cust.CustomerID
        and (wh.W_DTS)>= cust.EffectiveDate 
        and (wh.W_DTS)<= cust.EndDate
    join master.DimSecurity s on wh.W_S_SYMB=s.Symbol
        and (wh.W_DTS)>= s.EffectiveDate 
        and (wh.W_DTS)<= s.EndDate
    join master.DimDate d on DATE(wh.W_DTS) = 	d.DateValue
    where s.IsCurrent and cust.IsCurrent
  )

select
  wh.SK_CustomerID as SK_CustomerID,
  wh.SK_SecurityID as SK_SecurityID,
  wh.SK_DateID as SK_DateID_DatePlaced,
  wh2.SK_DateID as SK_DateID_DateRemoved,
  1 as BatchID
from (select * from watch_hist where W_ACTION = 'ACTV') wh
left join (select * from watch_hist where W_ACTION = 'CNCL') wh2
    on wh.SK_CustomerID = wh2.SK_CustomerID
    and wh.SK_SecurityID = wh2.SK_SecurityID
;