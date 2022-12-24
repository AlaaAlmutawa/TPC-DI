/***
4.5.10 FactHoldings
4.5.10.1 Data for FactHoldings comes from the HoldingHistory.txt file and the DimTrade table. 
The
quantity and price values reflect the holdings for a particular security after the most recent trade. 
The customer can have a positive or negative position (Quantity) as a result of a trade
4.5.10.2 When populating fields of the FactHoldings table:
Retrieve the following values from DimTrade where HH_T_ID (current trade identifier) from the HoldingHistory.txt file matches the TradeID from DimTrade:
SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID and CurrentPrice
SK_DateID is set to the value of SK_CloseDateID and SK_TimeID is set to the value of
SK_CloseTimeID
TradeId and CurrentTradeID values are supplied by HH_H_T_ID and HH_T_ID
CurrentHolding – this value is supplied by HH_AFTER_QTY
BatchID is set as described in section 4.4.2.
***/
/**
The
quantity and price values reflect the holdings for a particular security after the most recent trade. 
The customer can have a positive or negative position (Quantity) as a result of a trade

HH_H_T_ID -> TradeID ((dimtrade))
HH_T_ID -> CurrentTradeID (((holdinghistory.txt)))
SK_CustomerID ((dimtrade))
SK_AccountID ((dimtrade))
SK_SecurityID ((dimtrade))
SK_CompanyID ((dimtrade))
CurrentPrice ((dimtrade))
SK_CloseDateID -> SK_DateID ((dimtrade))
SK_CloseTimeID -> SK_TimeID ((dimtrade))
HH_AFTER_QTY -> CurrentHolding (((holdinghistory.txt)))
1 -> BatchID
***/

--insert into master.FactHoldings
with agg1 as (
  select 
    hh.HH_H_T_ID as TradeID 
  , hh.HH_T_ID as CurrentTrade
  , SK_CustomerID
  , SK_AccountID
  , SK_SecurityID
  , SK_CompanyID
  , SK_CloseDateID as SK_DateID
  , SK_CloseTimeID as SK_TimeID
  , BidPrice as CurrentPrice
  , hh.HH_AFTER_QTY as CurrentHolding
  , row_number() over(partition by SK_CustomerID, SK_AccountID, SK_SecurityID, SK_CompanyID order by DateValue desc) as row_num
  from master.DimTrade dt
  join staging.holding_history hh on hh.HH_T_ID = dt.TradeID
  join master.DimDate dd on dd.SK_DateID = dt.SK_CloseDateID 
)
, get_recent_holding as (
  select 
    TradeID 
  , CurrentTrade
  , SK_CustomerID
  , SK_AccountID
  , SK_SecurityID
  , SK_CompanyID
  , SK_DateID
  , SK_TimeID
  , CurrentPrice
  , CurrentHolding
  , 1 as BatchID
  from agg1
  where row_num = 1
)
select * from get_recent_holding;







