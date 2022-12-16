---Dimension Account
DROP TABLE IF EXISTS master.DimAccount;

CREATE TABLE master.DimAccount (
    SK_AccountID INT64 NOT NULL,
    AccountID INT64 NOT NULL,
    SK_BrokerID INT64 NOT NULL,
    SK_CustomerID INT64 NOT NULL,
    Status STRING(10) NOT NULL,
    AccountDesc STRING(50) ,
    TaxStatus INT64 NOT NULL,
    IsCurrent BOOLEAN NOT NULL,
    BatchID INT64 NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL
);

---Dimension Broker
DROP TABLE IF EXISTS master.DimBroker;

CREATE TABLE master.DimBroker (
    SK_BrokerID INT64 NOT NULL,
    BrokerID INT64 NOT NULL,
    ManagerID INT64,
    FirstName STRING(50) NOT NULL,
    LastName STRING(50) NOT NULL,
    MiddleInitial STRING(1),
    Branch STRING(50) ,
    Office STRING(50),
    Phone STRING(14),
    IsCurrent BOOLEAN NOT NULL,
    BatchID INT64 NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL    
);

---Dimension Company
DROP TABLE IF EXISTS master.DimCompany;

CREATE TABLE master.DimCompany (
    SK_CompanyID INT64 NOT NULL,
    CompanyID INT64 NOT NULL,
    Status STRING(10) NOT NULL,
    Name STRING(60) NOT NULL,
    Industry STRING(50) NOT NULL,
    SPrating STRING(4),
    IsLowGrade BOOLEAN,
    CEO STRING(100) NOT NULL,
    AddressLine1 STRING(80),
    AddressLine2 STRING(80),
    PostalCode STRING(12) NOT NULL,
    City STRING(25) NOT NULL,
    StateProv STRING(20) NOT NULL,
    Country STRING(24),
    Description STRING(150) NOT NULL,
    FoundingDate DATE,
    IsCurrent BOOLEAN NOT NULL,
    BatchID INT64 NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL  
);

---Dimension Customer
DROP TABLE IF EXISTS master.DimCustomer;

CREATE TABLE master.DimCustomer (
    SK_CustomerID INT64 NOT NULL,
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
    StateProv STRING(20) NOT NULL,
    Country STRING(24),
    Phone1 STRING(30),
    Phone2 STRING(30),
    Phone3 STRING(30),
    Email1 STRING(50),
    Email2 STRING(50),
    NationalTaxRateDesc STRING(50),
    NationalTaxRate NUMERIC(6,5),
    LocalTaxRateDesc STRING(50),
    LocalTaxRate NUMERIC(6,5),
    AgencyID STRING(30),
    CreditRating INT64,
    NetWorth INT64,
    MarketingNameplate STRING(100),
    IsCurrent BOOLEAN NOT NULL,
    BatchID INT64 NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL 
);

---Dimension Date
DROP TABLE IF EXISTS master.DimDate;

CREATE TABLE master.DimDate (
    SK_DateID INT64 NOT NULL,
    DateValue DATE NOT NULL,
    DateDesc STRING(20) NOT NULL,
    CalendarYearID INT64 NOT NULL,
    CalendarYearDesc STRING(20) NOT NULL,
    CalendarQtrID INT64 NOT NULL,
    CalendarQtrDesc STRING(20) NOT NULL,
    CalendarMonthID INT64 NOT NULL,
    CalendarMonthDesc STRING(20) NOT NULL,
    CalendarWeekID INT64 NOT NULL,
    CalendarWeekDesc STRING(20) NOT NULL,
    DayOfWeekNum INT64 NOT NULL,
    DayOfWeekDesc STRING(10) NOT NULL,
    FiscalYearID INT64 NOT NULL,
    FiscalYearDesc STRING(20) NOT NULL,
    FiscalQtrID INT64 NOT NULL,
    FiscalQtrDesc STRING(20) NOT NULL,
    HolidayFlag BOOLEAN
);

---Dimension Security
DROP TABLE IF EXISTS master.DimSecurity;

CREATE TABLE master.DimSecurity (
    SK_SecurityID INT64 NOT NULL,
    Symbol STRING(15) NOT NULL,
    Issue STRING(6) NOT NULL,
    Status STRING(10) NOT NULL,
    Name STRING(70) NOT NULL,
    ExchangeID STRING(6) NOT NULL,
    SK_CompanyID INT64 NOT NULL,
    SharesOutstanding INT64 NOT NULL,
    FirstTrade DATE NOT NULL,
    FirstTradeOnExchange  DATE NOT NULL,
    Dividend NUMERIC(10,2) NOT NULL,
    IsCurrent BOOLEAN NOT NULL,
    BatchID INT64 NOT NULL,
    EffectiveDate DATE NOT NULL,
    EndDate DATE NOT NULL   
);

---Dimension Time
DROP TABLE IF EXISTS master.DimTime;

CREATE TABLE master.DimTime (
    SK_TimeID INT64 NOT NULL,
    TimeValue TIME NOT NULL,
    HourID INT64 NOT NULL,
    HourDesc STRING(20) NOT NULL,
    MinuteID INT64 NOT NULL,
    MinuteDesc STRING(20) NOT NULL,
    SecondID INT64 NOT NULL,
    SecondDesc STRING(20) NOT NULL,
    MarketHoursFlag BOOLEAN,
    OfficeHoursFlag BOOLEAN
);

---Dimension Trade
DROP TABLE IF EXISTS master.DimTrade;

CREATE TABLE master.DimTrade (
    TradeID INT64 NOT NULL,
    SK_BrokerID INT64,
    SK_CreateDateID INT64 NOT NULL,
    SK_CreateTimeID INT64 NOT NULL,
    SK_CloseDateID INT64,
    SK_CloseTimeID INT64,
    Status STRING(10) NOT NULL,
    Type STRING(12) NOT NULL,
    CashFlag BOOLEAN NOT NULL,
    SK_SecurityID INT64 NOT NULL,
    SK_CompanyID INT64 NOT NULL,
    Quantity NUMERIC(6,0) NOT NULL,
    BidPrice NUMERIC(8,2) NOT NULL,
    SK_CustomerID INT64 NOT NULL,
    SK_AccountID INT64 NOT NULL,
    ExecutedBy STRING(64) NOT NULL,
    TradePrice NUMERIC(8,2),
    Fee NUMERIC(10,2),
    Commission NUMERIC(10,2),
    Tax NUMERIC(10,2),
    BatchID INT64 NOT NULL
);

---Dimension Messages
DROP TABLE IF EXISTS master.DimMessages;

CREATE TABLE master.DimMessages (
    MessageDateAndTime DATETIME NOT NULL,
    BatchID INT64 NOT NULL,
    MessageSource STRING(30),
    MessageText STRING NOT NULL,            --purposefully not limited
    MessageType STRING(12) NOT NULL,
    MessageData STRING                      --purposefully not limited
);

---Fact Cash Balances
DROP TABLE IF EXISTS master.FactCashBalances;

CREATE TABLE master.FactCashBalances (
    SK_CustomerID INT64 NOT NULL,
    SK_AccountID INT64 NOT NULL,
    SK_DateID INT64 NOT NULL,
    Cash NUMERIC(15,2) NOT NULL,
    BatchID INT64 NOT NULL
);

---Fact Holdings
DROP TABLE IF EXISTS master.FactHoldings;

CREATE TABLE master.FactHoldings (
    TradeID INT64 NOT NULL,
    CurrentTradeID INT64 NOT NULL,
    SK_CustomerID INT64 NOT NULL,
    SK_AccountID INT64 NOT NULL,
    SK_SecurityID INT64 NOT NULL,
    SK_CompanyID INT64 NOT NULL,
    SK_DateID INT64 NOT NULL,
    SK_TimeID INT64 NOT NULL,
    CurrentPrice NUMERIC(8,2) ,
    CurrentHolding INT64 NOT NULL,
    BatchID INT64 NOT NULL
);

---Fact Market History
DROP TABLE IF EXISTS master.FactMarketHistory;

CREATE TABLE master.FactMarketHistory (
    SK_SecurityID INT64 NOT NULL,
    SK_CompanyID INT64 NOT NULL,
    SK_DateID INT64 NOT NULL,
    PERatio NUMERIC(10,2) ,
    Yield NUMERIC(5,2) NOT NULL,
    FiftyTwoWeekHigh NUMERIC(8,2) NOT NULL,
    SK_FiftyTwoWeekHighDate INT64 NOT NULL,
    FiftyTwoWeekLow NUMERIC(8,2) NOT NULL,
    SK_FiftyTwoWeekLowDate INT64 NOT NULL,
    ClosePrice NUMERIC(8,2) NOT NULL,
    DayHigh NUMERIC(8,2) NOT NULL,
    DayLow NUMERIC(8,2) NOT NULL,
    Volume INT64 NOT NULL,
    BatchID INT64 NOT NULL
);

---Fact Watches 
DROP TABLE IF EXISTS master.FactWatches;

CREATE TABLE master.FactWatches (
    SK_CustomerID INT64 NOT NULL,
    SK_SecurityID INT64 NOT NULL,
    SK_DateID_DatePlaced INT64 NOT NULL,
    SK_DateID_DateRemoved INT64,
    BatchID INT64 NOT NULL
);

---Prospect 
DROP TABLE IF EXISTS master.Prospect;

CREATE TABLE master.Prospect (
    AgencyID STRING(30) NOT NULL,
    SK_RecordDateID INT64 NOT NULL,
    SK_UpdateDateID INT64 NOT NULL,
    BatchID INT64 NOT NULL,
    IsCustomer BOOLEAN NOT NULL,
    LastName STRING(30) NOT NULL,
    FirstName STRING(30) NOT NULL,
    MiddleInitial STRING(1),
    Gender STRING(1),
    AddressLine1 STRING(80),
    AddressLine2 STRING(80),
    PostalCode STRING(12),
    City STRING(25) NOT NULL,
    State STRING(20) NOT NULL,
    Country STRING(24),
    Phone STRING(30),
    Income INT64,
    NumberCars INT64,
    NumberChildren INT64,
    MaritalStatus STRING(1),
    Age INT64,
    CreditRating INT64,
    OwnOrRentFlag STRING(1),
    Employer STRING(30),
    NumberCreditCards INT64,
    NetWorth INT64,
    MarketingNameplate STRING(100)
);

---Audit 
DROP TABLE IF EXISTS master.Audit;

CREATE TABLE master.Audit (
    DataSet STRING(20) NOT NULL,
    BatchID INT64,
    Date DATE,
    Attribute STRING(50) NOT NULL,
    Value INT64,
    DValue NUMERIC(15,5)
);

---Reference Trade Type
DROP TABLE IF EXISTS master.TradeType;

CREATE TABLE
  master.TradeType( 
    -- Trade type code
    TT_ID STRING(3) NOT NULL,
    -- Trade type description
    TT_NAME STRING(12) NOT NULL,
    -- Flag indicating a sale
    TT_IS_SELL INT64 NOT NULL,
    -- Flag indicating a market order
    TT_IS_MRKT INT64 NOT NULL
    );

---Reference Industry 
DROP TABLE IF EXISTS master.industry;

CREATE TABLE
  master.Industry ( 
    --Industry code 
    IN_ID STRING(2) NOT NULL,
    --Industry description
    IN_NAME STRING(50) NOT NULL, 
    --Sector Identifier 
    IN_SC_ID STRING(4) NOT NULL 

  );

---Reference Status Type
DROP TABLE IF EXISTS master.StatusType;

CREATE TABLE
  master.StatusType( 
    --Status code
    ST_ID STRING(4) NOT NULL , 
    --Status description
    ST_NAME STRING(10) NOT NULL 
  );

--Reference Tax Rate
DROP TABLE IF EXISTS master.TaxRate;

CREATE TABLE
  master.TaxRate ( 
    --Tax rate code
    TX_ID STRING(4) NOT NULL ,
    --Tax rate description
    TX_NAME STRING(50) NOT NULL ,
    -- Tax rate
    TX_RATE NUMERIC(6,5) NOT NULL 
  );
