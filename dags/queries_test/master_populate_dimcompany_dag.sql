/**
DimCompany data is obtained from the FINWIRE files. All FINWIREyyyyQq files are processed
in ascending year and quarter order, and records of type CMP are used. 
CMP records may have content that is unchanged from prior CMP records, except for the PTS field. 
Changes to DimCompany are implemented in a history-tracking manner, but unchanged records are not recorded. 
CIK is the natural key for the Company data.
When populating fields of the DimCompany table:
- CompanyID is copied from CIK.
- Name, SPRating, CEO, Description and FoundingDate are copied from CompanyName,
SPrating, CEOname, Description, and FoundingDate respectively. In cases where the input
data is all blanks, a NULL value is used in the target.
- AddressLine1, AddressLine2, PostalCode, City, State_Prov, and Country are copied from
AddrLine1, AddrLine2, PostalCode, City, StateProvince, and Country. In cases where the input
data is all blanks, a NULL value is used in the target.
- Status is obtained from the FINWIRE Status by matching Status with ST_ID from the StatusType.txt file.
- Industry is obtained from IndustryID by matching IndustryID with IN_ID from the Industry.txt file.
- isLowGrade is set to False if SPrating begins with ‘A’ or ‘BBB’ otherwise set to True
- IsCurrent, EffectiveDate and EndDate are set as described in section 4.4.1, with
EffectiveDate being the date indicated by the PTS field.
- BatchID is set as described in section 4.4.2.

**/

CREATE TEMP FUNCTION getStatus(status STRING)
    RETURNS STRING 
    AS ((
        SELECT ST_NAME 
        FROM master1.StatusType
        WHERE ST_ID = status

    ));
    CREATE TEMP FUNCTION getIndustry(i STRING)
    RETURNS STRING 
    AS ((
        SELECT IN_NAME 
        FROM master1.Industry
        WHERE IN_ID = i

    ));
--insert into master1.DimCompany  
  select 
  CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', DATE(PTS)), '', CAST(CIK AS STRING)) AS INT64) as SK_CompanyID,
  CAST(CIK as INT64) as CompanyID,
  getStatus(Status) as Status, 
  CompanyName as Name,
  getIndustry(IndustryID) as Industry, 
  SPrating as SPRating,
  CASE 
      WHEN SPrating LIKE 'A%' OR SPrating LIKE 'BBB%' 
      THEN False
      ELSE True 
  END
  AS isLowGrade,  
  CEOname as CEO,
  AddrLine1 as AddressLine1, 
  AddrLine2 as AddressLine2,  
  PostalCode, 
  City,
  StateProvince as StatProv,
  Country,
  Description,
  FoundingDate, 
  True AS IsCurrent,
  1 AS BatchID,
  DATE(PTS) AS EffectiveDate, 
  CAST('9999-12-31' as DATE) AS EndDate
  FROM staging1.finwire_cmp_record
  WHERE RecType = 'CMP'
