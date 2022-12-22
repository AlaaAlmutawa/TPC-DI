/*
Data from: CustomerMgmt.xml

1. When ./@ActionType is ‘NEW’ or ‘ADDACCT’

2. AccountID, AccountDesc and TaxStatus fields are copied from
Customer/Account/@CA_ID, Customer/Account/CA_NAME and
Customer/Account/@CA_TAX_ST respectively.

3. When ./@ActionType is ‘CLOSEACCT’

master.dim_account(SK_AccountID INT64 NOT NULL,
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


-- insert into master.DimAccount
select 
as
SK_AccountID
    c.Customer.Account.CA_B_ID as SK_BrokerID,
    c.Customer.attr_C_ID as SK_CustomerID,
c.Customer.Account.attr_CA_ID as AccountID,
    'ACTIVE' as Status,
c.Customer.Account.CA_NAME as AccountDesc,
c.Customer.Account.attr_CA_TAX_ST as TaxStatus,
    True IsCurrent,
    1 as BatchID,
    c.Customer.ActionTS as EffectiveDate,
    --Beginning of date range when this record was the current record
    EndDate
    c.Customer.ActionType as Action
from staging.customer_management c
where c.Customer.ActionType in ("NEW", "ADDACCT");