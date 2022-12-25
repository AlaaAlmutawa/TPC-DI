--Loading txt files into staging
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

CREATE TABLE 
    staging.batch_date (
        BatchDate DATE
    );

--CashTransaction.txt -> 2.2.5

CREATE TABLE 
    staging.cash_transaction (
        CT_CA_ID INT64,
        CT_DTS DATETIME,
        CT_AMT FLOAT64,
        CT_NAME STRING
    );

--DailyMarket.txt

CREATE TABLE 
    staging.daily_market (
        DM_DATE DATE,
        DM_S_SYMB STRING,
        DM_CLOSE NUMERIC,
        DM_HIGH NUMERIC,
        DM_LOW NUMERIC,
        DM_VOL INT64
    );

-- HoldingHistory.txt 2.2.9

CREATE TABLE 
    staging.holding_history (
        HH_H_T_ID INT64,
        HH_T_ID INT64,
        HH_BEFORE_QTY INT64,
        HH_AFTER_QTY INT64
    );

-- Trade.txt -> table 2.2.16

CREATE TABLE 
    staging.trade (
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

CREATE TABLE 
    staging.trade_history (
        TH_T_ID INT64,
        TRADE_T DATETIME,
        TH_ST_ID STRING
    );

-- WatchHistory.txt -> table 2.2.19

CREATE TABLE 
    staging.watch_history (
        W_C_ID INT64,
        W_S_SYMB STRING,
        W_DTS DATETIME,
        W_ACTION STRING
    );

-- Prospect.csv -> table 2.2.12 

CREATE TABLE 
    staging.prospect (
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

CREATE TABLE 
    staging.hr (
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

-- CustomerManagement.xml temporary loaded
CREATE TABLE staging.customer_historical (
    CustomerID INT64 NOT NULL,
    TaxID STRING(20) NOT NULL,
    Status STRING(10) NOT NULL,
    LastName STRING(30) NOT NULL,
    FirstName STRING(30) NOT NULL,
    MiddleInitial STRING(1),
    Gender STRING(1),
    Tier INT64,
    DOB DATE,
    AddressLine1 STRING(80) NOT NULL,
    AddressLine2 STRING(80),
    PostalCode STRING(12) NOT NULL,
    City STRING(25) NOT NULL,
    State_Prov STRING(20) NOT NULL,
    Country STRING(24),
    Phone1 STRING(30),
    Phone2 STRING(30),
    Phone3 STRING(30),
    Email1 STRING(50),
    Email2 STRING(50),
    NationalTaxID STRING(20),
    LocalTaxID STRING(20),
    Action STRING(10),
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL 
);



