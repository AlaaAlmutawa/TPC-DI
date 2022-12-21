
/**
Reference tables:
Date.txt - ok
Time.txt - ok
Industry.txt - ok
StatusType.txt - ok
TaxRate.txt - ok
TradeType.txt -ok
**/
---- Schema of TradeType table -> Refer Page 46 3.2.18
CREATE TABLE
  staging.trade_type ( 
    -- Trade type code
    TT_ID STRING,
    -- Trade type description
    TT_NAME STRING,
    -- Flag indicating a sale
    TT_IS_SELL INT64,
    -- Flag indicating a market order
    TT_IS_MRKT INT64 
    );

-- Schema of the dim_date -> pg 40 3.2.5 . 
CREATE TABLE
  staging.dim_date ( 
    -- Surrogate key for the date
    SK_DateID INT64 ,
    --  The date as text, e.g. “2004-07-07”
    DateValue DATE ,
    --The date Month Day, YYYY, e.g. July 7, 2004
    DateDesc STRING ,
    -- Year number as a number
    CalendarYearID INT64 ,
    -- Year number as text
    CalendarYearDesc STRING ,
    -- Quarter as a number, e.g. 20042
    CalendarQtrID INT64 ,
    -- Quarter as text, e.g. “2004 Q2”
    CalendarQtrDesc STRING ,
    -- Month as a number, e.g. 20047
    CalendarMonthID INT64 ,
    -- Month as text, e.g. “2004 July”
    CalendarMonthDesc STRING ,
    -- Week as a number, e.g. 200428
    CalendarWeekID INT64 ,
    -- Week as text, e.g. “2004-W28”
    CalendarWeekDesc STRING ,
    -- Day of week as a number, e.g. 3
    DayOfWeekNum INT64 ,
    -- Day of week as text, e.g. “Wednesday”
    DayOfWeekDesc STRING ,
    -- Fiscal year as a number, e.g. 2005
    FiscalYearID INT64 ,
    -- Fiscal year as text, e.g. “2005”
    FiscalYearDesc STRING ,
    -- Fiscal quarter as a number, e.g. 20051
    FiscalQtrID INT64 ,
    -- Fiscal quarter as text, e.g. “2005 Q1”
    FiscalQtrDesc STRING ,
    -- Indicates holidays
    HolidayFlag BOOLEAN 
    );

--schema of dim_time pg 41 3.2.7 

CREATE TABLE
  staging.dim_time ( 
    --Surrogate key for the time
    SK_TimeID INT64,
    --The time stored appropriately for doing comparisons in the Data Warehouse 
    TimeValue TIME,
    --Hour number as a number, e.g. 01
    HourID INT64,
    --Hour number as text, e.g. “01”
    HourDesc STRING, 
    --Minute as a number, e.g. 23
    MinuteID INT64, 
    --Minute as text, e.g. “01:23”
    MinuteDesc STRING, 
    --Second as a number, e.g. 45
    SecondID INT64, 
    --Second as text, e.g. “01:23:45” 
    SecondDesc STRING, 
    --Indicates a time during market hours 
    MarketHoursFlag BOOLEAN, 
    --Indicates a time during office hours
    OfficeHoursFlag BOOLEAN 
  );

-- Schema of industry pg 44 3.2.13
CREATE TABLE
  staging.industry ( 
    --Industry code 
    IN_ID STRING,
    --Industry description
    IN_NAME STRING, 
    --Sector Identifier 
    IN_SC_ID STRING 

  );

-- Schema for status_type pg 45 sect 3.2.16
CREATE TABLE
  staging.status_type ( 
    --Status code
    ST_ID STRING , 
    --Status description
    ST_NAME STRING 
  );

-- Schema for tax_rate pg 46 sect 3.2.17
CREATE TABLE
  staging.tax_rate ( 
    --Tax rate code
    TX_ID STRING ,
    --Tax rate description
    TX_NAME STRING ,
    -- Tax rate
    TX_RATE NUMERIC 
  );

-- Schema for customer historical temp table
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
-- General schema for FINWIRE files 

CREATE TABLE 
  staging.finwire (
    RECORD STRING
  );




