--Loading txt files into staging1
/**
OLTP system are:
Account.txt (Incremental Update) X
Customer.txt (Incremental Update) X
Trade.txt (Historical Load and Incremental Update) X
TradeHistory.txt (Historical Load) X
CashTransaction.txt (Historical Load and Incremental Update) X
HoldingHistory.txt (Historical Load and Incremental Update) X
DailyMarket.txt (Historical Load and Incremental Update) X
WatchHistory.txt (Historical Load and Incremental Update) X
Prospect.csv
**/
-- BatchDate.txt -> 
DROP TABLE IF EXISTS staging1.batch_date;

CREATE TABLE 
    staging1.batch_date (
        BatchDate DATE
    );

--CashTransaction.txt -> 2.2.5
DROP TABLE IF EXISTS staging1.cash_transaction;

CREATE TABLE 
    staging1.cash_transaction (
        CT_CA_ID INT64,
        CT_DTS DATETIME,
        CT_AMT FLOAT64,
        CT_NAME STRING
    );

--DailyMarket.txt
DROP TABLE IF EXISTS staging1.daily_market;

CREATE TABLE 
    staging1.daily_market (
        DM_DATE DATE,
        DM_S_SYMB STRING,
        DM_CLOSE NUMERIC,
        DM_HIGH NUMERIC,
        DM_LOW NUMERIC,
        DM_VOL INT64
    );

-- HoldingHistory.txt 2.2.9
DROP TABLE IF EXISTS staging1.holding_history;

CREATE TABLE 
    staging1.holding_history (
        HH_H_T_ID INT64,
        HH_T_ID INT64,
        HH_BEFORE_QTY INT64,
        HH_AFTER_QTY INT64
    );

-- Trade.txt -> table 2.2.16
DROP TABLE IF EXISTS staging1.trade;

CREATE TABLE 
    staging1.trade (
        T_ID INT64,
        T_DTS DATETIME,
        T_ST_ID STRING,
        T_TT_ID STRING,
        T_IS_CASH BOOLEAN,
        T_S_SYMB STRING,
        T_QTY INT64,
        T_BID_PRICE NUMERIC,
        T_CA_ID INT64,
        T_EXEC_NAME STRING,
        T_TRADE_PRICE NUMERIC,
        T_CHRG NUMERIC,
        T_COMM NUMERIC,
        T_TAX NUMERIC
    );

-- TradeHistory.txt -> table 2.2.15
DROP TABLE IF EXISTS staging1.trade_history;

CREATE TABLE 
    staging1.trade_history (
        TH_T_ID INT64,
        TRADE_T DATETIME,
        TH_ST_ID STRING
    );

-- WatchHistory.txt -> table 2.2.19
DROP TABLE IF EXISTS staging1.watch_history;

CREATE TABLE 
    staging1.watch_history (
        W_C_ID INT64,
        W_S_SYMB STRING,
        W_DTS DATETIME,
        W_ACTION STRING
    );

-- Prospect.csv -> table 2.2.12 
DROP TABLE IF EXISTS staging1.prospect;

CREATE TABLE 
    staging1.prospect (
        AgencyID STRING, 
        LastName STRING, 
        FirstName STRING, 
        MiddleInitial STRING, 
        Gender STRING, 
        AddressLine1 STRING, 
        AddressLine2 STRING, 
        PostalCode STRING, 
        City STRING, 
        State STRING, 
        Country STRING, 
        Phone STRING,
        Income NUMERIC, 
        NumberCars INT64, 
        NumberChildern INT64, 
        MaritalStatus STRING, 
        Age INT64, 
        CreditRating NUMERIC, 
        OwnOrRentFlag STRING, 
        Employer STRING, 
        NumberCreditCards INT64, 
        NetWorth INT64 
    );

DROP TABLE IF EXISTS staging1.hr;

CREATE TABLE 
    staging1.hr (
        EmployeeID INT64,
        ManagerID INT64,
        EmployeeFirstName STRING, 
        EmployeeLastName STRING, 
        EmployeeMI STRING,
        EmployeeJobCode INT64,
        EmployeeBranch STRING,
        EmployeeOffice STRING,
        EmployeePhone STRING

    );

DROP TABLE IF EXISTS staging1.finwire;

CREATE TABLE 
    staging1.finwire (
        RECORD STRING

    );


