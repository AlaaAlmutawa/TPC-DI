/*
DimCompany records in DIMessages described in 4.5.3.3
*/

SELECT
    CURRENT_DATETIME() AS MessageDateAndTime,
    1 AS BatchID,
    "Alert" as MessageType,
     "DimCompany" AS MessageSource,
    error.MessageText AS MessageText,
    error.MessageData AS MessageData
FROM (
         SELECT
             "No earnings for company" AS MessageText,
             CONCAT(CONCAT("CO_ID=", '', CAST(CompanyID AS STRING), ',', CONCAT("CO_SP_RATE=", '', CAST(SPrating AS STRING)))) AS MessageData
         FROM
             `master.DimCompany`
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
                            "D")) error;