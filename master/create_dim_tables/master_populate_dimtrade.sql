-- insert into master.DimTrade

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
  FROM staging.trade as trade
  JOIN staging.trade_history as trade_history ON trade.T_ID = trade_history.TH_T_ID

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
  JOIN `master.DimAccount` acc
  ON acc.AccountID = lt.CAID AND extract(date from (lt.createdTimestamp)) >= acc.EffectiveDate AND extract(date from (lt.createdTimestamp)) < acc.EndDate

  JOIN `master.StatusType` sty 
  ON sty.ST_ID = lt.Status

  JOIN `master.TradeType` tt
  ON tt.TT_ID = lt.Type

  JOIN `master.DimSecurity` sec
  ON sec.Symbol = lt.Symbol AND DATE(lt.createdTimestamp) >= sec.EffectiveDate AND DATE(lt.createdTimestamp) < sec.EndDate

  left JOIN `master.DimTime` crt
  ON crt.TimeValue = TIME_TRUNC(extract(time from (lt.createdTimestamp)), SECOND)

  JOIN `master.DimDate` crd
  ON crd.DateValue = extract(date from (lt.createdTimestamp))

  LEFT JOIN `master.DimTime` clt
  ON clt.TimeValue= TIME_TRUNC(extract(time from (lt.closedTimestamp)), SECOND)

  LEFT JOIN `master.DimDate` cld
  ON cld.DateValue = extract(date from (lt.closedTimestamp));





