/*
DimCustomer records in DIMessages described in 4.5.4.4
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" as MessageType,
     "DimCustomer" AS MessageSource,
    error.MessageText AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "Invalid Customr tier" AS MessageText,
             CONCAT(CONCAT("C_ID=", '', CAST(CustomerID AS STRING), ',', CONCAT("C_TIER=", '', CAST(Tier AS STRING)))) AS MessageData
         FROM
             `master.DimCustomer`
         WHERE 
            Tier NOT IN (1, 2, 3)) error;


/*
DimCustomer records in DIMessages described in 4.5.4.5
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" as MessageType,
    "DimCustomer" AS MessageSource,
    error.MessageText AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "DOB out of range" AS MessageText,
             CONCAT(CONCAT("C_ID=", '', CAST(CustomerID AS STRING), ',', CONCAT("C_DOB=", '', CAST(DOB AS STRING)))) AS MessageData
         FROM
             `master.DimCustomer`, `staging.batch_date`
         WHERE 
             DATE_DIFF(BatchDate, DOB, YEAR) > 100
             or
             DOB > BatchDate) error;



