/*
master1.dim_account(
    SK_AccountID INT64 NOT NULL,
    --Surrogate key for AccountID
    AccountID INT64 NOT NULL,
    --Customer account identifier
    SK_BrokerID INT64 NOT NULL,
    --Surrogate key of managing broker
    SK_CustomerID INT64 NOT NULL,
    --Surrogate key of customer
    Status STRING NOT NULL,
    --Account status, active or closed
    AccountDesc STRING,
    --Name of customer account
    TaxStatus INT64 NOT NULL,
    --0, 1 or 2 Tax status of this account
    IsCurrent BOOLEAN NOT NULL,
    --True if this is the current record
    BatchID INT64 NOT NULL,
    --Batch ID when this record was inserted
    EffectiveDate DATE NOT NULL,
    --Beginning of date range when this record was the current record
    EndDate DATE NOT NULL
    --Ending of date range when this record was the current record. A record that is not expired willuse the date 9999-12-31.
    );
*/

-- insert into master1.DimAccount

with account_history as 
(
select 
    CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', c.Customer.ActionTS), '', CAST(c.Customer.Account.attr_CA_ID AS STRING)) AS INT64) as SK_AccountID,
    LAST_VALUE(b.SK_BrokerID IGNORE NULLS) OVER acct AS SK_BrokerID,
    cust.SK_CustomerID as SK_CustomerID,
    c.Customer.Account.attr_CA_ID as AccountID,
    CASE WHEN Customer.ActionTYPE IN ("INACT", "CLOSEACCT") THEN "INACTIVE"
        ELSE "ACTIVE"
        END AS Status,
    LAST_VALUE(c.Customer.Account.CA_NAME IGNORE NULLS) OVER acct AS AccountDesc,
    LAST_VALUE(c.Customer.Account.attr_CA_TAX_ST IGNORE NULLS) OVER acct AS TaxStatus,

    1 as BatchID,
    DATE(c.Customer.ActionTS) as EffectiveDate,
    LEAD(DATE(c.Customer.ActionTS), 1, DATE("9999-12-31")) 
        OVER acct as EndDate,
    (c.Customer.ActionTS) as ActionDate
from staging1.customer_management c
left join master1.DimBroker b on  b.BrokerID = c.Customer.Account.CA_B_ID
    and DATE(c.Customer.ActionTS)>= b.EffectiveDate 
    and DATE(c.Customer.ActionTS)<= b.EndDate
left join master1.DimCustomer cust on cust.CustomerID = c.Customer.attr_C_ID 
    and DATE(c.Customer.ActionTS)>= cust.EffectiveDate 
    and DATE(c.Customer.ActionTS)<= cust.EndDate
where c.Customer.ActionType in ("NEW","ADDACCT", "UPDACCT", "CLOSEACCT", "INACT")

WINDOW
    acct AS 
    (PARTITION BY c.Customer.Account.attr_CA_ID 
        ORDER BY c.Customer.ActionTS)
)

select 
    SK_AccountID,
    AccountID,
    SK_BrokerID,
    SK_CustomerID,
    Status,
    AccountDesc,
    TaxStatus,
    CASE WHEN EndDate = DATE('9999-12-31') THEN TRUE
      ELSE FALSE
      END as IsCurrent,
    BatchID,
    EffectiveDate,
    EndDate
from account_history
where EffectiveDate!=EndDate and AccountID is not null
;



