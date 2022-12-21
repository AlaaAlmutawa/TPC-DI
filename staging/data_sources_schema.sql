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


--Account.txt -> table 2.2.3 --> this is in batch2 and batch3
CREATE TABLE 
    staging.account(

    CDC_FLAG STRING,
    CDC_DSN INT64,
    CA_ID INT64,
    CA_B_ID INT64,
    CA_C_ID INT64,
    CA_NAME STRING,
    CA_TAX_ST INT64,
    CA_ST_ID STRING

);

-- Customer.txt -> table 2.2.6 --> this is in batch2 and batch3 
CREATE TABLE 
    staging.customer(
        CDC_FLAG STRING,
        CDC_DSN INT64,
        C_ID INT64,
        C_TAX_ID STRING,
        C_ST_ID STRING,
        C_L_NAME STRING,
        C_F_NAME STRING,
        C_M_NAME STRING,
        C_GNDR STRING,
        C_TIER INT64,
        C_DOB DATE,
        C_ADLINE1 STRING,
        C_ADLINE2 STRING,
        C_ZIPCODE STRING,
        C_CITY STRING,
        C_STATE_PROV STRING,
        C_CTRY STRING,
        C_CTRY_1 STRING,
        C_AREA_1 STRING,
        C_LOCAL_1 STRING,
        C_EXT_1 STRING,
        C_CTRY_2 STRING,
        C_AREA_2 STRING,
        C_LOCAL_2 STRING,
        C_EXT_2 STRING,
        C_CTRY_3 STRING,
        C_AREA_3 STRING,
        C_LOCAL_3 STRING,
        C_EXT_3 STRING,
        C_EMAIL_1 STRING,
        C_EMAIL_2 STRING,
        C_LCL_TX_ID STRING,
        C_NAT_TX_ID STRING
    );

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


