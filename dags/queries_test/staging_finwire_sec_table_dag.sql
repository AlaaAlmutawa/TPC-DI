--sec records
-- Table 2.2.8

--create table staging.finwire_sec_record as 
select 
  parse_datetime('%E4Y%m%d-%H%M%S',substr(record,1,15)) AS PTS, 
  trim(substr(record,16,3)) AS RecType, 
  trim(substr(record,19,15)) AS Symbol, 
  trim(substr(record,34,6)) AS IssueType, 
  trim(substr(record,40,4)) AS Status, 
  trim(substr(record,44,70)) AS Name, 
  trim(substr(record,114,6)) AS ExID, 
  cast(trim(substr(record,120,13)) as INT64) AS ShOut, 
  parse_date('%E4Y%m%d',trim(substr(record,133,8))) AS FirstTradeDate,
  parse_date('%E4Y%m%d',trim(substr(record,141,8))) AS FirstTradeExchg, 
  cast(trim(substr(record,149,12)) as numeric) AS Dividend, 
  case 
    when char_length(trim(substr(record,161,60))) >= 10 then trim(substr(record,161,60))
    ELSE NULL
  END AS CompanyName,
  case 
    when char_length(trim(substr(record,161,60))) <= 10 then SAFE_CAST(trim(substr(record,161,60)) AS INT64)
    ELSE NULL
  END AS CIK
  from 
  staging1.finwire
  where 
  trim(substr(record,16,3)) = 'SEC';