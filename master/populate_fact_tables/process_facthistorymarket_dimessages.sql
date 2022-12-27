/*
FactMarketHistory records in DIMessages described in 4.5.11.3
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" AS MessageType,
    "FactMarketHistory" AS MessageSource,
    error.MessageText AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "No earnings for company" AS MessageText,
             CONCAT(CONCAT("DM_S_SYMB=", '', CAST(s.Symbol AS STRING))) AS MessageData
         FROM
             `master.FactMarketHistory` fmh
             JOIN
             `master.DimSecurity` s
             ON fmh.SK_SecurityID = s.SK_SecurityID
         WHERE 
             ) error;


