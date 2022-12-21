--fin records
-- Table 2.2.8

--create table staging.finwire_fin_record as 
select 
  parse_datetime('%E4Y%m%d-%H%M%S',substr(record,1,15)) AS PTS, 
  trim(substr(record,16,3)) AS RecType, 
  cast(trim(substr(record,19,4)) as INT64) AS Year, 
  cast(trim(substr(record,23,1)) as INT64) AS Quarter, 
  parse_date('%E4Y%m%d',trim(substr(record,24,8))) AS QtrStartDate, 
  parse_date('%E4Y%m%d',trim(substr(record,32,8))) AS PostingDate, 
  cast(trim(substr(record,40,17)) as numeric) AS Revenue, 
  cast(trim(substr(record,57,17)) as numeric) AS Earnings, 
  cast(trim(substr(record,74,12)) as numeric) AS EPS,
  cast(trim(substr(record,86,12)) as numeric) AS DilutedEPS, 
  cast(trim(substr(record,98,12)) as numeric) AS Margin, 
  cast(trim(substr(record,110,17)) as numeric) AS Inventory, 
  cast(trim(substr(record,127,17)) as numeric) AS Assets,
  cast(trim(substr(record,144,17)) as numeric) AS Liabilities,
  cast(trim(substr(record,161,13)) as INT64) AS ShOut,
  cast(trim(substr(record,174,13)) as INT64) AS DilutedShOut,
  case 
    when char_length(trim(substr(record,187,60))) >= 10 then trim(substr(record,187,60))
    ELSE NULL
  END AS CompanyName,
  case 
    when char_length(trim(substr(record,187,60))) <= 10 then CAST(trim(substr(record,187,60)) AS INT64)
    ELSE NULL
  END AS CIK
  from 
  staging.finwire
  where 
  trim(substr(record,16,3)) = 'FIN';