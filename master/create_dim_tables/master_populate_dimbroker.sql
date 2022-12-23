--section 4.5.2
--staging.hr -> DimBroker 
--EmployeeJobCode = 314
--EffectiveDate -> min(date) from DimDate
--BatchID -> 1 
--IsCurrent = True 

--insert into master.DimBroker
with minDate as (
    select min(DateValue) as EffectiveDate from master.DimDate
)
select 
CAST(CONCAT(FORMAT_DATE('%E4Y%m%d', minDate.EffectiveDate), '', CAST(EmployeeID AS STRING)) AS INT64)  as SK_BrokerID,
EmployeeID as BrokerID, 
ManagerID, 
EmployeeFirstName as FirstName, 
EmployeeLastName as LastName, 
EmployeeMI as MiddleInitial, 
EmployeeBranch as Branch, 
EmployeeOffice as Office, 
EmployeePhone as Phone,
True as IsCurrent,
1 as BatchID, 
minDate.EffectiveDate as EffectiveDate,
CAST('9999-12-31' as DATE) as EndDate
from staging.hr 
cross join minDate
where EmployeeJobCode = 314
