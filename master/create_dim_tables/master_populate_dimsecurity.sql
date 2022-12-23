/***
DimSecurity
4.5.6.1 DimSecurity data is obtained from the FINWIRE files. All FINWIREyyyyQq files are processed
in ascending year and quarter order, and records of type SEC are used. 
The surrogate key of the associated company must be obtained for the Company dimension reference. 
Changes to DimSecurity are implemented in a history-tracking manner. Symbol is the natural key for the Security data.
4.5.6.2 When populating fields of the DimSecurity table:
Symbol, Issue, Name, ExchangeID, SharesOutstanding, 
FirstTrade, FirstTradeOnExchange and Dividend are copied from Symbol, IssueType, Name, ExID, ShOut, FirstTradeDate, FirstTradeExchg and Dividend 
respectively from the SEC record.
SK_CompanyID is obtained from the DimCompany table by matching CoNameOrCIK with Name or CIKcode (depending on the characters found in CoNameOrCIK), 
where PTS >= EffectiveDate and PTS < EndDate, to return the SK_CompanyID. 
The match is guaranteed to succeed due to the integrity of the FINWIRE data. This dependency of DimSecurity on DimCompany requires that 
any update to a company’s DimCompany records must be completed before updates to that company’s DimSecurity records.
Status is obtained from the StatusType table by matching Status from the FINWIRE record with ST_ID to return the ST_NAME.
IsCurrent, EffectiveDate, and EndDate are set as described in section 4.4.1, where the EffectiveDate is the date indicated by the PTS field.
BatchID is set as described in section 4.4.2.
***/

CREATE TEMP FUNCTION getStatus(status STRING)
    RETURNS STRING 
    AS ((
        SELECT ST_NAME 
        FROM master.StatusType
        WHERE ST_ID = status

    ));
--insert into master.DimSecurity  
  select 
  FARM_FINGERPRINT(CONCAT(DATE(PTS), '', Symbol)) AS SK_SecurityID,
  sec.Symbol, 
  sec.IssueType AS Issue, 
  getStatus(sec.Status) AS Status,
  sec.Name, 
  sec.ExID AS ExchangeID, 
  SK_CompanyID,
  sec.ShOut AS SharesOutstanding, 
  sec.FirstTradeDate AS FirstTrade,
  sec.FirstTradeExchg AS FirstTradeOnExchange, 
  sec.Dividend,
  True AS IsCurrent,
  1 AS BatchID,
  DATE(PTS) AS EffectiveDate, 
  CAST('9999-12-31' as DATE) AS EndDate
  FROM staging.finwire_sec_record sec 
  INNER JOIN master.DimCompany cmp on
  CASE 
    WHEN sec.CIK is not NULL 
    THEN sec.CIK = cmp.CompanyID
    ELSE sec.CompanyName = cmp.Name
    END 
  WHERE
    sec.PTS >= cmp.EffectiveDate
    AND sec.PTS < cmp.EndDate
