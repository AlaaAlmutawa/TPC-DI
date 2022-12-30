/***

DimTrade
4.5.8 DimTrade data is obtained from the Trade.txt and TradeHistory.txt. 
- If TH_ST_ID is 'SBMT' and T_TT_ID is either 'TMB' or 'TMS', or TH_ST_ID is 'PNDG',
then SK_CreateDateID and SK_CreateTimeID must be set based on TH_DTS, with time
truncated to 1-second resolution. SK_CloseDateID and SK_CloseTimeID must be set to
NULL if a new DimTrade record is being inserted.
- If TH_ST_ID is 'CMPT' or 'CNCL', SK_CloseDateID and SK_CloseTimeID must be set based
on TH_DTS, with time truncated to 1-second resolution. SK_CreateDateID and
SK_CreateTimeID must be set to NULL if a new DimTrade record is being inserted.
- TradeID, CashFlag, Quantity, BidPrice, ExecutedBy, TradePrice, Fee, Commission and Tax
are copied from T_ID, T_IS_CASH, T_QTY, T_BID_PRICE, T_EXEC_NAME, T_TRADE_PRICE,
T_CHRG, T_COMM and T_TAX respectively.
- Status is copied from ST_NAME of the StatusType table by matching T_ST_ID with ST_ID.
- Type is copied from TT_NAME of the TradeType table by matching T_TT_ID with TT_ID.
- SK_SecurityID and SK_CompanyID are copied from SK_SecurityID and SK_CompanyID of
the DimSecurity table by matching T_S_SYMB with Symbol where TH_DTS is in the range
given by EffectiveDate and EndDate. The match is guaranteed to succeed due to the
referential integrity of the OLTP database. Note that these surrogate key values must
reference the dimension record that is current at the earliest time this TradeID is
encountered. If an update to a record is required in order to set the SK_CloseDateID and
SK_CloseTimeID, these fields must not be updated. This dependency of DimTrade on
DimSecurity requires that any update to a security's DimSecurity records must be
completed before updates to that security's DimTrade records.
- SK_AccountID, SK_CustomerID, and SK_BrokerID are copied from the SK_AccountID,
SK_CustomerID, and SK_BrokerID fields of the DimAccount table by matching T_CA_ID
with AccountID where TH_DTS is in the range given by EffectiveDate and EndDate. The
match is guaranteed to succeed due to the referential integrity of the OLTP database.
Note that these surrogate key values must reference the dimension record that is current
at the earliest time this TradeID is encountered. If an update to a record is required in
order to set the SK_CloseDateID and SK_CloseTimeID, these fields must not be updated.
This dependency of DimTrade on DimAccount requires that any update to an account's
DimAccount records must be completed before updates to that account's DimTrade
records.
BatchID is set as described in section 4.4.2 at the time the record is initially created.

***/


-- insert into master1.DimTrade

WITH base_trade as(
  SELECT trade.T_ID as TradeID,
  trade.T_DTS as TradeTimestamp,
  trade.T_ST_ID as Status, 
  trade.T_TT_ID as Type, 
  trade.T_IS_CASH as CashFlag,
  trade.T_S_SYMB as Symbol,
  trade.T_QTY as Quantity,
  trade.T_BID_PRICE as BidPrice,
  trade.T_CA_ID as CAID,
  trade.T_EXEC_NAME as ExecutedBy,
  trade.T_TRADE_PRICE as TradePrice,
  trade.T_CHRG as Fee,
  trade.T_COMM as Commission,
  trade.T_TAX as Tax,
  IF ((trade_history.TH_ST_ID = 'SBMT' and (trade.T_TT_ID = 'TMB' or trade.T_TT_ID = 'TMS'))
  or trade_history.TH_ST_ID = 'PNDG', trade_history.TRADE_T, NULL) as createdTimestamp,
  IF (trade_history.TH_ST_ID = 'CMPT'or trade_history.TH_ST_ID = 'CNCL',
  trade_history.TRADE_T, NULL) as closedTimestamp,
  trade_history.TRADE_T as actionTime
  FROM staging1.trade as trade
  JOIN staging1.trade_history as trade_history ON trade.T_ID = trade_history.TH_T_ID

),

 latest_trades as (

    SELECT TradeID,
    Status,
    Type,
    CashFlag,
    Symbol,
    Quantity,
    BidPrice,
    CAID,
    ExecutedBy,
    TradePrice,
    Fee,
    Commission,
    Tax,
    createdTimestamp,
    closedTimestamp

    FROM (
      SELECT TradeID,
      FIRST_VALUE(Status) OVER w as Status,
      FIRST_VALUE(Type) OVER w as Type,
      FIRST_VALUE(CashFlag) OVER w as CashFlag,
      FIRST_VALUE(Symbol) OVER w as Symbol,
      FIRST_VALUE(Quantity) OVER w as Quantity,
      FIRST_VALUE(BidPrice) OVER w as BidPrice,
      FIRST_VALUE(CAID) OVER w as CAID,
      FIRST_VALUE(ExecutedBy) OVER w as ExecutedBy,
      FIRST_VALUE(TradePrice) OVER w as TradePrice,
      FIRST_VALUE(Fee) OVER w as Fee,
      FIRST_VALUE(Commission) OVER w as Commission,
      FIRST_VALUE(Tax) OVER w as Tax,
      LAST_VALUE(createdTimestamp) OVER
      (w RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as createdTimestamp,
      FIRST_VALUE(closedTimestamp) OVER w as closedTimestamp,
      ROW_NUMBER() OVER w as time_order

      FROM base_trade WINDOW w as (
        PARTITION BY TradeID
        ORDER BY actionTime DESC 
      ) 
    ) a

    WHERE a.time_order=1

  )
  
  
  SELECT lt.TradeID as TradeID,
       acc.SK_BrokerID as SK_BrokerID,
       crd.SK_DateID as SK_CreateDateID,
       crt.SK_TimeID as SK_CreateTimeID,
       cld.SK_DateID as SK_CloseDateID,
       clt.SK_TimeID as SK_CloseTimeID,
       sty.ST_NAME as Status,
       tt.TT_NAME as Type,
       lt.CashFlag as CashFlag,
       sec.SK_SecurityID as SK_SecurityID,
       sec.SK_CompanyID as SK_CompanyID,
       lt.Quantity as Quantity,
       lt.BidPrice as BidPrice,
       acc.SK_CustomerID as SK_CustomerID,
       acc.SK_AccountID  as SK_AccountID,
       lt.ExecutedBy as ExecutedBy,
       lt.TradePrice as TradePrice,
       lt.Fee as Fee,
       lt.Commission as Commission,
       lt.Tax as Tax,
       1 as BatchID

  FROM latest_trades lt 
  JOIN master1.DimAccount acc
  ON acc.AccountID = lt.CAID AND extract(date from (lt.createdTimestamp)) >= acc.EffectiveDate AND extract(date from (lt.createdTimestamp)) < acc.EndDate

  JOIN master1.StatusType sty 
  ON sty.ST_ID = lt.Status

  JOIN master1.TradeType tt
  ON tt.TT_ID = lt.Type

  JOIN master1.DimSecurity sec
  ON sec.Symbol = lt.Symbol AND DATE(lt.createdTimestamp) >= sec.EffectiveDate AND DATE(lt.createdTimestamp) < sec.EndDate

  left JOIN master1.DimTime crt
  ON crt.TimeValue = TIME_TRUNC(extract(time from (lt.createdTimestamp)), SECOND)

  JOIN master1.DimDate crd
  ON crd.DateValue = extract(date from (lt.createdTimestamp))

  LEFT JOIN master1.DimTime clt
  ON clt.TimeValue= TIME_TRUNC(extract(time from (lt.closedTimestamp)), SECOND)

  LEFT JOIN master1.DimDate cld
  ON cld.DateValue = extract(date from (lt.closedTimestamp));





