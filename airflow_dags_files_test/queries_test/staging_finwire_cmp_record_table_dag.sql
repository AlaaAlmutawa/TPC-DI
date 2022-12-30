--cmp records
-- Table 2.2.8
--create table staging.finwire_cmp_record as 
  select 
  parse_datetime('%E4Y%m%d-%H%M%S',substr(record,1,15)) AS PTS, 
  trim(substr(record,16,3)) AS RecType, 
  trim(substr(record,19,60)) AS CompanyName, 
  trim(substr(record,79,10)) AS CIK, 
  trim(substr(record,89,4)) AS Status, 
  trim(substr(record,93,2)) AS IndustryID, 
  trim(substr(record,95,4)) AS SPrating, 
  case 
    when trim(substr(record,99,8)) = '' then null
    else parse_date('%E4Y%m%d',trim(substr(record,99,8))) 
    end
    AS FoundingDate, 
  trim(substr(record,107,80)) AS AddrLine1,
  case 
    when trim(substr(record,187,80)) = '' then null
    else trim(substr(record,187,80)) 
    end 
    AS AddrLine2, 
  trim(substr(record,267,12)) AS PostalCode, 
  trim(substr(record,279,25)) AS City, 
  trim(substr(record,304,20)) AS StateProvince, 
  case 
    when trim(substr(record,324,24)) = '' then null
    else trim(substr(record,324,24)) 
    end 
    AS Country, 
  trim(substr(record,348,46)) AS CEOname, 
  trim(substr(record,394,150)) AS Description
  from 
  staging1.finwire
  where 
  trim(substr(record,16,3)) = 'CMP';


