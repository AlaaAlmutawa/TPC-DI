
/*
-- Schema of FactCashBalances table -> Refer Page 43 3.2.9
DROP TABLE IF EXISTS master.fact_cash_balances;
CREATE TABLE
  master.fact_cash_balances(SK_CustomerID INT64 NOT NULL,
    --Surrogate key for CustomerID
    SK_AccountID INT64 NOT NULL,
    --Surrogate key for AccountID
    SK_DateID INT64 NOT NULL,
    --Surrogate key for the date
    Cash NUMERIC NOT NULL,
    --Cash balance for the account after applying changes for this day
    BatchID INT64 NOT NULL
    --Batch ID when this record was inserted
    );
*/

-- insert into master.FactCashBalances
with cash_totals as
  (
    select CT_CA_ID, DATE(CT_DTS) as CT_DTS,
          sum(CT_AMT) as Cash
    from staging1.cash_transaction
    group by 1, 2
    )

select 
    a.SK_CustomerID as SK_CustomerID,
    a.SK_AccountID as SK_AccountID,
    d.SK_DateID as SK_DateID,
    CAST(SUM(ct.Cash)
    OVER
      (PARTITION BY a.SK_AccountID
      ORDER BY d.SK_DateID ASC) AS NUMERIC) AS Cash,
    1 as BatchID 
from cash_totals ct
join master1.DimAccount a on ct.CT_CA_ID = a.AccountID
    and ct.CT_DTS<a.EndDate and ct.CT_DTS>=a.EffectiveDate
join master1.DimDate d on ct.CT_DTS = d.DateValue
;
