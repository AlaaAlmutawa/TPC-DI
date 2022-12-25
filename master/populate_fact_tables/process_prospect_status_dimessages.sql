/*
DimCustomer records in DIMessages described in 4.5.15.2
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Status" as MessageType,
    "Prospect" AS MessageSource,
    error.MessageText AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "Inserted rows" AS MessageText,
             CONCAT(CONCAT("Number of rows=", '', CAST(count(*) AS STRING))) AS MessageData
         FROM
             `staging.prospect`) error;