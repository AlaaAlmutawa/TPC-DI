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
        BatchDate DATE NOT NULL
    );

--CashTransaction.txt -> 2.2.5
DROP TABLE IF EXISTS staging1.cash_transaction;

CREATE TABLE 
    staging1.cash_transaction (
        CT_CA_ID INT64 NOT NULL,
        CT_DTS DATETIME NOT NULL,
        CT_AMT FLOAT64 NOT NULL,
        CT_NAME STRING NOT NULL
    );

--DailyMarket.txt
DROP TABLE IF EXISTS staging1.daily_market;

CREATE TABLE 
    staging1.daily_market (
        DM_DATE DATE NOT NULL,
        DM_S_SYMB STRING NOT NULL,
        DM_CLOSE NUMERIC NOT NULL,
        DM_HIGH NUMERIC NOT NULL,
        DM_LOW NUMERIC NOT NULL,
        DM_VOL INT64 NOT NULL
    );

-- HoldingHistory.txt 2.2.9
DROP TABLE IF EXISTS staging1.holding_history;

CREATE TABLE 
    staging1.holding_history (
        HH_H_T_ID INT64 NOT NULL,
        HH_T_ID INT64 NOT NULL,
        HH_BEFORE_QTY INT64 NOT NULL,
        HH_AFTER_QTY INT64 NOT NULL
    );

-- Trade.txt -> table 2.2.16
DROP TABLE IF EXISTS staging1.trade;

CREATE TABLE 
    staging1.trade (
        T_ID INT64 NOT NULL,
        T_DTS DATETIME NOT NULL,
        T_ST_ID STRING NOT NULL,
        T_TT_ID STRING NOT NULL,
        T_IS_CASH BOOLEAN NOT NULL,
        T_S_SYMB STRING NOT NULL,
        T_QTY INT64 NOT NULL,
        T_BID_PRICE NUMERIC NOT NULL,
        T_CA_ID INT64 NOT NULL,
        T_EXEC_NAME STRING NOT NULL,
        T_TRADE_PRICE NUMERIC,
        T_CHRG NUMERIC,
        T_COMM NUMERIC,
        T_TAX NUMERIC
    );

-- TradeHistory.txt -> table 2.2.15
DROP TABLE IF EXISTS staging1.trade_history;

CREATE TABLE 
    staging1.trade_history (
        TH_T_ID INT64 NOT NULL,
        TRADE_T DATETIME NOT NULL,
        TH_ST_ID STRING NOT NULL
    );

-- WatchHistory.txt -> table 2.2.19
DROP TABLE IF EXISTS staging1.watch_history;

CREATE TABLE 
    staging1.watch_history (
        W_C_ID INT64 NOT NULL,
        W_S_SYMB STRING NOT NULL,
        W_DTS DATETIME NOT NULL,
        W_ACTION STRING NOT NULL
    );

-- Prospect.csv -> table 2.2.12 
DROP TABLE IF EXISTS staging1.prospect;

CREATE TABLE 
    staging1.prospect (
        AgencyID STRING NOT NULL, 
        LastName STRING NOT NULL, 
        FirstName STRING NOT NULL, 
        MiddleInitial STRING, 
        Gender STRING, 
        AddressLine1 STRING, 
        AddressLine2 STRING, 
        PostalCode STRING, 
        City STRING NOT NULL, 
        State STRING NOT NULL, 
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
        EmployeeID INT64 NOT NULL,
        ManagerID INT64 NOT NULL,
        EmployeeFirstName STRING NOT NULL, 
        EmployeeLastName STRING NOT NULL, 
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


