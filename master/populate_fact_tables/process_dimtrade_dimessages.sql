/*
DimTrade records in DIMessages described in 4.5.8.3
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" AS MessageType,
    "DimTrade" AS MessageSource,
    "Invalide trade commission" AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "Invalid trade commission" AS MessageText,
             CONCAT(CONCAT("T_ID=", '', CAST(TradeID AS STRING)), ',', CONCAT("T_COMM=", '', CAST(Commission AS STRING))) AS MessageData
         FROM
             master.DimTrade
         WHERE 
             Commission IS NOT NULL and Commission > TradePrice * Quantity) error;


/*
DimTrade records in DIMessages described in 4.5.8.4
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" as MessageType,
    "DimTrade" AS MessageSource,
    "Invalide trade fee" AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "Invalid trade commission" AS MessageText,
             CONCAT(CONCAT("T_ID=", '', CAST(TradeID AS STRING)), ',', CONCAT("T_CHRG=", '', CAST(Fee AS STRING))) AS MessageData
         FROM
             master.DimTrade
         WHERE 
             Fee IS NOT NULL and Fee > TradePrice * Quantity) error;


