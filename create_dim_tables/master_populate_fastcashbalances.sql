
/*
-- Schema of CashTransaction table -> Refer Page 22 2.2.2.3.1
CREATE TABLE
  staging.cash_transaction ( CDC_FLAG STRING NOT NULL,
    -- L ‘I’	Denotes insert
    CDC_DSN INT64 NOT NULL,
    --	Database Sequence Number
    CT_CA_ID INT64 NOT NULL,
    --	Customer account identifier
    CT_DTS DATETIME NOT NULL,
    --	Timestamp of when the trade took place
    CT_AMT FLOAT64 NOT NULL,
    --	Amount of the cash transaction.
    CT_NAME STRING NOT NULL --	Transaction name, or description: e.g. “Cash from sale of DuPont stock”.
    );


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
(select CT_CA_ID, FORMAT_DATE('%E4Y-%m-%d', CT_DTS) as CT_DTS,
ROUND(sum(CT_AMT),2) as Cash
from staging.cash_transaction
group by CT_CA_ID, FORMAT_DATE('%E4Y-%m-%d', CT_DTS))

select 
    a.SK_CustomerID as SK_CustomerID,
    a.SK_AccountID as SK_AccountID,
    d.SK_DateID as SK_DateID,
    ct.Cash as Cash,
    1 as BatchID 
from cash_totals ct
join master.DimAccount a on ct.CT_CA_ID = a.AccountID
join master.DimDate d on CAST(ct.CT_DTS AS DATE) = d.DateValue
where ct.CT_DTS<EndDate and ct.CT_DTS>=Effective_Date
;
