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








