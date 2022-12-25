--4.6.4 DimCustomer pg 72

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

--INSERT INTO `master.DimCustomer`
SELECT 
CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', EffectiveDate), '', CAST(CustomerID AS STRING)) AS INT64)  as SK_CustomerID,
c.CustomerID as CustomerID,
c.TaxID as TaxID,
c.Status as Status,
c.LastName as LastName,
c.FirstName as FirstName,
c.MiddleInitial as MiddleInitial,
c.Gender as Gender,
c.Tier as Tier,
c.DOB as DOB,
c.AddressLine1 as AddressLine1,
c.AddressLine2 as AddressLine2,
c.PostalCode as PostalCode,
c.City as City,
c.State_Prov as StateProv,
c.Country as Country,
c.Phone1 as Phone1,
c.Phone2 as Phone2,
c.Phone3 as Phone3,
c.Email1 as Email1,
c.Email2 as Email2,
n.TX_NAME as NationalTaxRateDesc,
n.TX_RATE as NationalTaxRate,
l.TX_NAME as LocalTaxRateDesc,
l.TX_RATE as LocalTaxRate,
p.AgencyID as AgencyID,
CAST(p.CreditRating AS INT64) as CreditRating,
p.NetWorth as NetWorth,
construct_marketing_nameplate(p.NetWorth, p.Income, p.NumberChildern, p.NumberCreditCards, p.Age, p.CreditRating, p.NumberCars) as MarketingNameplate,
c.EndDate = DATE('9999-12-31') as IsCurrent,
1 as BatchID,
c.EffectiveDate as EffectiveDate,
c.EndDate as EndDate
FROM
`staging.customer_historical` c 
LEFT JOIN `master.TaxRate` n ON (c.NationalTaxID = n.TX_ID)
LEFT JOIN `master.TaxRate` l ON (c.LocalTaxID = l.TX_ID)
LEFT JOIN `staging.prospect` p ON (UPPER(p.LastName) = UPPER(c.LastName) 
  AND UPPER(p.FirstName) = UPPER(c.FirstName)
  AND UPPER(p.AddressLine1) = UPPER(c.AddressLine1)
  AND UPPER(p.AddressLine2) = UPPER(c.AddressLine2)
  AND UPPER(p.PostalCode) = UPPER(c.PostalCode)
  AND c.EndDate = DATE('9999-12-31'))
;

