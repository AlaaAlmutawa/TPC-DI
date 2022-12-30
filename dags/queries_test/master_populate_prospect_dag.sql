--4.6.14 Prospect pg 78

CREATE TEMPORARY FUNCTION construct_marketing_nameplate( NetWorth FLOAT64,  Income FLOAT64,  NumberChildren FLOAT64,  NumberCreditCards FLOAT64,  Age FLOAT64,  CreditRating FLOAT64,  NumberCars FLOAT64   ) 
RETURNS STRING
LANGUAGE js AS """
tags = [];
   if((NetWorth != null && NetWorth > 1000000) || (Income != null && Income > 200000)) tags.push("HighValue");
   if((NumberChildren != null && NumberChildren >3) || (NumberCreditCards != null && NumberCreditCards > 5)) tags.push("Expenses");
   if(Age != null && Age>45) tags.push("Boomer");
   if((Income != null && Income < 50000) || (CreditRating != null && CreditRating < 600) || (NetWorth != null && NetWorth<100000)) tags.push("MoneyAlert");
   if((NumberCars != null && NumberCars>3) || (NumberCreditCards != null && NumberCreditCards > 7)) tags.push("Spender");
   if((Age != null && Age <25) || (NetWorth != null && NetWorth>1000000)) tags.push("Inherited");

    if(tags.length==0) return null;

   nameplate = tags.join("+");
   return nameplate;
   """;


--INSERT INTO `master.Prospect`

WITH batch_date AS (
  SELECT d.SK_DateID as SK_DateID
  FROM master1.DimDate d JOIN 
  staging1.batch_date batch ON
  d.DateValue = batch.BatchDate
)
SELECT 
AgencyID,
b.SK_DateID as SK_RecordDateID,
b.SK_DateID as SK_UpdateDateID,
1 as BatchID,
ch.CustomerID IS NOT NULL as IsCustomer,
p.LastName as LastName,
p.FirstName as FirstName,
p.MiddleInitial as MiddleInitial,
p.Gender as Gender,
p.AddressLine1 as AddressLine1,
p.AddressLine2 as AddressLine2,
p.PostalCode as PostalCode,
p.City as City,
p.State as State,
p.Country as Country,
p.Phone as Phone,
CAST(p.Income as INT64) as Income,
p.NumberCars as NumberCars,
p.NumberChildern as NumberChildren,
p.MaritalStatus as MaritalStatus,
p.Age as Age,
CAST(p.CreditRating as INT64) as CreditRating,
p.OwnOrRentFlag as OwnOrRentFlag,
p.Employer as Employer,
p.NumberCreditCards as NumberCreditCards,
p.NetWorth as NetWorth,
construct_marketing_nameplate(p.NetWorth, p.Income, p.NumberChildern, p.NumberCreditCards, p.Age, p.CreditRating, p.NumberCars) as MarketingNameplate
 FROM
staging1.prospect p CROSS JOIN batch_date b
LEFT JOIN staging1.customer_historical ch ON
  UPPER(p.FirstName) = UPPER(ch.FirstName) AND
  UPPER(p.LastName) = UPPER(ch.LastName) AND 
  UPPER(p.AddressLine1) = UPPER(ch.AddressLine1) AND
  UPPER(p.AddressLine2) = UPPER(ch.AddressLine2) AND 
  UPPER(p.PostalCode) = UPPER(ch.PostalCode) AND
  ch.Status = 'ACTIVE'


