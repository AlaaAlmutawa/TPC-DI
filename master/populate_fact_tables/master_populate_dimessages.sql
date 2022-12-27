/*
DimCompany records in DIMessages described in 4.5.3.3
*/

INSERT INTO `master.DimMessages`
SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "DimCompany" AS MessageSource,
    "Invalid SPRating" AS MessageText,
    "Alert" as MessageType,
    CONCAT(CONCAT("CO_ID=", '', CAST(CIK AS STRING), ',', CONCAT("CO_SP_RATE=", '', CAST(SPrating AS STRING)))) AS MessageData
FROM 
    `staging.finwire_cmp_record`
WHERE 
    SPrating NOT IN ("AAA",
                    "AA",
                    "AA+",
                    "AA-",
                    "A",
                    "A+",
                    "A-",
                    "BBB",
                    "BBB+",
                    "BBB-",
                    "BB",
                    "BB+",
                    "BB-",
                    "B",
                    "B+",
                    "B-",
                    "CCC",
                    "CCC+",
                    "CCC-",
                    "CC",
                    "C",
                    "D");


SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "DimCustomer" AS MessageSource,
    error.MessageText AS MessageText,
    "Alert" as MessageType,
    error.MessageData AS MessageData
FROM (
    /*
    DimCustomer records in DIMessages described in 4.5.4.4
    */
         SELECT
             "Invalid Customr tier" AS MessageText,
             CONCAT(CONCAT("C_ID=", '', CAST(CustomerID AS STRING), ',', CONCAT("C_TIER=", '', CAST(Tier AS STRING)))) AS MessageData
         FROM
             `staging.customer_historical`
         WHERE 
            Tier NOT IN (1, 2, 3)
            
            
        UNION ALL 
        

    /*
    DimCustomer records in DIMessages described in 4.5.4.5
    */
        SELECT
            "DOB out of range" AS MessageText,
            CONCAT(CONCAT("C_ID=", '', CAST(CustomerID AS STRING), ',', CONCAT("C_DOB=", '', CAST(DOB AS STRING)))) AS MessageData
        FROM
            `staging.customer_historical`, `staging.batch_date`
        WHERE 
            DATE_DIFF(BatchDate, DOB, YEAR) > 100
            or
            DOB > BatchDate) error;





SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "DimTrade" AS MessageSource,
    "Invalide trade commission" AS MessageText,
    "Alert" AS MessageType,
    error.MessageData AS MessageData
FROM (
        /*
        DimTrade records in DIMessages described in 4.5.8.3
        */
         SELECT
             "Invalid trade commission" AS MessageText,
             CONCAT(CONCAT("T_ID=", '', CAST(TradeID AS STRING)), ',', CONCAT("T_COMM=", '', CAST(Commission AS STRING))) AS MessageData
         FROM
             `master.DimTrade`
         WHERE 
             Commission IS NOT NULL and Commission > TradePrice * Quantity

         UNION ALL   

        /*
        DimTrade records in DIMessages described in 4.5.8.4
        */ 
        
         SELECT
             "Invalid trade commission" AS MessageText,
             CONCAT(CONCAT("T_ID=", '', CAST(TradeID AS STRING)), ',', CONCAT("T_CHRG=", '', CAST(Fee AS STRING))) AS MessageData
         FROM
             `master.DimTrade`
         WHERE 
             Fee IS NOT NULL and Fee > TradePrice * Quantity) error;



/*
DimCustomer records in DIMessages described in 4.5.15.2
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Prospect" AS MessageSource,
    "Inserted rows" AS MessageText,
    "Status" as MessageType,
    CONCAT("Number of rows=", '', CAST(count(*) AS STRING)) AS MessageData
FROM 
    `staging.prospect`;



/*
FactMarketHistory records in DIMessages described in 4.5.11.3
*/
INSERT INTO `master.DimMessages`
SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" AS MessageType,
    "FactMarketHistory" AS MessageSource,
    "No earnings for company" AS MessageText,
    CONCAT("DM_S_SYMB=", '', CAST(s.Symbol AS STRING)) AS MessageData
FROM 
    `master.FactMarketHistory` fmh
    JOIN
    `master.DimSecurity` s
    ON fmh.SK_SecurityID = s.SK_SecurityID
WHERE 
    fmh.PERatio is NULL;


