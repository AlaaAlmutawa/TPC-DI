/**
When populating fields of the Financial table:
FI_YEAR, FI_QTR, FI_QTR_START_DATE, FI_REVENUE, FI_NET_EARN, FI_BASIC_EPS, FI_DILUT_EPS, FI_MARGIN, FI_INVENTORY, FI_ASSETS, FI_LIABILITY, 
FI_OUT_BASIC, and FI_OUT_DILUT are copied from Year, Quarter, QtrStartDate, Revenue, Earnings, EPS, DilutedEPS, Margin, Inventory, Assets, 
Liabilities, ShOut, and DilutedShOut.

SK_CompanyID is obtained from the DimCompany table by matching CoNameOrCIK with Name or CIKcode (depending on the characters found in CoNameOrCIK), 
where EffectiveDate <= PTS < EndDate, to return the SK_CompanyID. The match is guaranteed to succeed due to the integrity of the FINWIRE data. 
This dependency of Financial on DimCompany requires that any update to a company’s DimCompany records must be completed before updates to that company’s 
Financial records.

**/

CREATE TEMP FUNCTION getStatus(status STRING)
    RETURNS STRING 
    AS ((
        SELECT ST_NAME 
        FROM master.StatusType
        WHERE ST_ID = status

    ));
insert into master.Financial
  select 
  SK_CompanyID
  , Year AS FI_YEAR 
  , Quarter AS FI_QTR
  , QtrStartDate AS FI_QTR_START_DATE
  , Revenue AS FI_REVENUE
  , Earnings AS FI_NET_EARN
  , EPS AS FI_BASIC_EPS
  , DilutedEPS AS FI_DILUT_EPS
  , Margin AS FI_MARGIN
  , Inventory AS FI_INVENTORY
  , Assets AS FI_ASSETS
  , Liabilities AS FI_LIABILITY
  , ShOut AS FI_OUT_BASIC
  , DilutedShOut AS FI_OUT_DILUT
  FROM staging.finwire_fin_record fin 
  INNER JOIN master.DimCompany cmp on
  CASE 
    WHEN fin.CIK is not NULL 
    THEN fin.CIK = cmp.CompanyID
    ELSE fin.CompanyName = cmp.Name
    END 
  WHERE
    fin.PTS >= cmp.EffectiveDate
    AND fin.PTS < cmp.EndDate