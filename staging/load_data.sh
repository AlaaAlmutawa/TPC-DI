#!/bin/bash
#this script was adjusted for our use case
#note that the script is expected .dat files (specifically to load store.dat into store table, and customer.dat into customer table)
# usage: ./load_data.sh $SCALE

#PROJECT=staging_${1}
PROJECT=staging #this is essentially your dataset name.

#GCS_BUCKET="tpc-ds-data-repo/${1}SF" #make sure that this matches your directory setup on google bucket

GCS_BUCKET="tpc_di_staging_files/sf${1}/Batch1"

GS_BASE=$GCS_BUCKET

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "load path: ${GS_BASE}"

#echo -e "${RED}Starting to delete Dataset: ${PROJECT}"

#bq rm -f $PROJECT

#echo -e "${GREEN}Starting to create Dataset: ${PROJECT}"

#bq mk $PROJECT ###SPECIFY LOCATION !!!!!!!!!

echo -e "${GREEN}Starting data load into Dataset: ${PROJECT} from GCS Path: ${GS_BASE}"

echo -e "${GREEN} Loading data into Table: ${PROJECT}.dim_date"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.dim_date gs://${GS_BASE}/Date.txt \
SK_DateID:integer,\
DateValue:date,\
DateDesc:string,\
CalendarYearID:integer,\
CalendarYearDesc:string,\
CalendarQtrID:integer,\
CalendarQtrDesc:string,\
CalendarMonthID:integer,\
CalendarMonthDesc:string,\
CalendarWeekID:integer,\
CalendarWeekDesc:string,\
DayOfWeekNum:integer,\
DayOfWeekDesc:string,\
FiscalYearID:integer,\
FiscalYearDesc:string,\
FiscalQtrID:integer,\
FiscalQtrDesc:string,\
HolidayFlag:boolean

echo -e "${GREEN} Loading data into Table: ${PROJECT}.trade_type"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.trade_type gs://${GS_BASE}/TradeType.txt \
TT_ID:string,\
TT_NAME:string,\
TT_IS_SELL:integer,\
TT_IS_MRKT:integer

echo -e "${GREEN} Loading data into Table: ${PROJECT}.dim_time"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.dim_time gs://${GS_BASE}/Time.txt \
SK_TimeID:integer,\
TimeValue:string,\
HourID:integer,\
HourDesc:string,\
MinuteID:integer,\
MinuteDesc:string,\
SecondID:integer,\
SecondDesc:string,\
MarketHoursFlag:boolean,\
OfficeHoursFlag:boolean


echo -e "${GREEN} Loading data into Table: ${PROJECT}.industry"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.industry gs://${GS_BASE}/Industry.txt \
IN_ID:string,\
IN_NAME:string,\
IN_SC_ID:string

echo -e "${GREEN} Loading data into Table: ${PROJECT}.status_type"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.status_type gs://${GS_BASE}/StatusType.txt \
ST_ID:string,\
ST_NAME:string

echo -e "${GREEN} Loading data into Table: ${PROJECT}.tax_rate"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.tax_rate gs://${GS_BASE}/TaxRate.txt \
TX_ID:string,\
TX_NAME:string,\
TX_RATE:numeric

echo -e "${GREEN} Loading data into Table: ${PROJECT}.batch_date"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.batch_date gs://${GS_BASE}/BatchDate.txt \
BatchDate:date

echo -e "${GREEN} Loading data into Table: ${PROJECT}.trade"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.trade gs://${GS_BASE}/Trade.txt \
T_ID:integer,\
T_DTS:datetime,\
T_ST_ID:string,\
T_TT_ID:string,\
T_IS_CASH:boolean,\
T_S_SYMB:string,\
T_QTY:integer,\
T_BID_PRICE:numeric,\
T_CA_ID:integer,\
T_EXEC_NAME:string,\
T_TRADE_PRICE:numeric,\
T_CHRG:numeric,\
T_COMM:numeric,\
T_TAX:numeric

echo -e "${GREEN} Loading data into Table: ${PROJECT}.cash_transaction"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.cash_transaction gs://${GS_BASE}/CashTransaction.txt \
CT_CA_ID:INT64,\
CT_DTS:DATETIME,\
CT_AMT:FLOAT64,\
CT_NAME:STRING

echo -e "${GREEN} Loading data into Table: ${PROJECT}.holding_history"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.holding_history gs://${GS_BASE}/HoldingHistory.txt \
HH_H_T_ID:INT64,\
HH_T_ID:INT64,\
HH_BEFORE_QTY:INT64,\
HH_AFTER_QTY:INT64

echo -e "${GREEN} Loading data into Table: ${PROJECT}.daily_market"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.daily_market gs://${GS_BASE}/DailyMarket.txt \
DM_DATE:DATE,\
DM_S_SYMB:STRING,\
DM_CLOSE:NUMERIC,\
DM_HIGH:NUMERIC,\
DM_LOW:NUMERIC,\
DM_VOL:INT64

echo -e "${GREEN} Loading data into Table: ${PROJECT}.trade_history"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.trade_history gs://${GS_BASE}/TradeHistory.txt \
TH_T_ID:INT64,\
TRADE_T:DATETIME,\
TH_ST_ID:STRING

echo -e "${GREEN} Loading data into Table: ${PROJECT}.watch_history"

bq load --project_id=tpc-di-370720 --field_delimiter '|' --null_marker='' --ignore_unknown_values ${PROJECT}.watch_history gs://${GS_BASE}/WatchHistory.txt \
W_C_ID:INT64,\
W_S_SYMB:STRING,\
W_DTS:DATETIME,\
W_ACTION:STRING


echo -e "${GREEN} Loading data into Table: ${PROJECT}.prospect"

bq load --project_id=tpc-di-370720 --field_delimiter ',' --null_marker='' --ignore_unknown_values ${PROJECT}.prospect gs://${GS_BASE}/Prospect.csv \
AgencyID:STRING,\
LastName:STRING,\
FirstName:STRING,\
MiddleInitial:STRING,\
Gender:STRING,\
AddressLine1:STRING,\
AddressLine2:STRING,\
PostalCode:STRING,\
City:STRING,\
State:STRING,\
Country:STRING,\
Phone:STRING,\
Income:NUMERIC,\
NumberCars:INT64,\
NumberChildern:INT64,\
MaritalStatus:STRING,\
Age:INT64,\
CreditRating:NUMERIC,\
OwnOrRentFlag:STRING,\
Employer:STRING,\
NumberCreditCards:INT64,\
NetWorth:INT64


echo -e "${GREEN} Loading data into Table: ${PROJECT}.hr"

bq load --project_id=tpc-di-370720 --field_delimiter ',' --null_marker='' --ignore_unknown_values ${PROJECT}.hr gs://${GS_BASE}/HR.csv \
EmployeeID:INT64,\
ManagerID:INT64,\
EmployeeFirstName:STRING,\
EmployeeLastName:STRING,\
EmployeeMI:STRING,\
EmployeeJobCode:INT64,\
EmployeeBranch:STRING,\
EmployeeOffice:STRING,\
EmployeePhone:STRING