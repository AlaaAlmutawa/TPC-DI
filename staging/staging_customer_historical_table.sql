CREATE TEMP FUNCTION construct_phone_number( phone_number ANY TYPE) AS (
  IF (phone_number IS NOT NULL,
    IF(phone_number.C_EXT IS NOT NULL,
      IF (phone_number.C_CTRY_CODE IS NOT NULL AND phone_number.C_AREA_CODE IS NOT NULL AND phone_number.C_LOCAL IS NOT NULL,
        CONCAT('+',phone_number.C_CTRY_CODE, ' (' , phone_number.C_AREA_CODE , ') ', phone_number.C_LOCAL, phone_number.C_EXT), 
        IF (phone_number.C_CTRY_CODE IS NULL AND phone_number.C_AREA_CODE IS NOT NULL AND phone_number.C_LOCAL IS NOT NULL ,
          CONCAT ( '(' , phone_number.C_AREA_CODE ,') ', phone_number.C_LOCAL, phone_number.C_EXT),
          IF (phone_number.C_CTRY_CODE IS NULL AND phone_number.C_AREA_CODE IS NULL AND phone_number.C_LOCAL IS NOT NULL,
            CONCAT(phone_number.C_LOCAL, phone_number.C_EXT),
            NULL
          )
        )
      ) ,
      IF (phone_number.C_CTRY_CODE IS NOT NULL AND phone_number.C_AREA_CODE IS NOT NULL AND phone_number.C_LOCAL IS NOT NULL,
      CONCAT('+',phone_number.C_CTRY_CODE, ' (' , phone_number.C_AREA_CODE , ') ', phone_number.C_LOCAL), 
      IF (phone_number.C_CTRY_CODE IS NULL AND phone_number.C_AREA_CODE IS NOT NULL AND phone_number.C_LOCAL IS NOT NULL ,
        CONCAT ( '(' , phone_number.C_AREA_CODE ,') ', phone_number.C_LOCAL),
        IF (phone_number.C_CTRY_CODE IS NULL AND phone_number.C_AREA_CODE IS NULL AND phone_number.C_LOCAL IS NOT NULL,
          phone_number.C_LOCAL,
          NULL
        )
      )
      )
    ),
    NULL
  )
);

--INSERT INTO `staging.customer_historical` 
WITH temp_customer AS(
  SELECT 
  Customer.ActionTS as effective_timestamp,
  Customer.attr_C_ID as CustomerID,
  Customer.attr_C_TAX_ID as TaxID,
  Customer.Name.C_L_NAME as LastName,
  Customer.Name.C_F_NAME as FirstName,
  Customer.Name.C_M_NAME as MiddleInitial,
  Customer.attr_C_TIER as Tier,
  Customer.attr_C_DOB as DOB,
  Customer.ContactInfo.C_PRIM_EMAIL as Email1,
  Customer.ContactInfo.C_ALT_EMAIL as Email2,
  CASE UPPER(Customer.attr_C_GNDR) 
    WHEN "M" THEN UPPER(Customer.attr_C_GNDR)
    WHEN "F" THEN UPPER(Customer.attr_C_GNDR)
    ELSE "U"
  END as Gender,
  Customer.Address.C_ADLINE1 as AddressLine1,
  Customer.Address.C_ADLINE2 as AddressLine2,
  Customer.Address.C_ZIPCODE as PostalCode,
  Customer.Address.C_CITY as City,
  Customer.Address.C_STATE_PROV as State_Prov,
  Customer.Address.C_CTRY as Country,
  CASE Customer.ActionType
    WHEN "INACT" THEN "INACTIVE"
    ELSE "ACTIVE"
  END as Status,
  Customer.TaxInfo.C_NAT_TX_ID as NationalTaxID,
  Customer.TaxInfo.C_LCL_TX_ID as LocalTaxID,
  construct_phone_number(Customer.ContactInfo.C_PHONE_1) as Phone1,
  construct_phone_number(Customer.ContactInfo.C_PHONE_2) as Phone2,
  construct_phone_number(Customer.ContactInfo.C_PHONE_3) as Phone3,
  Customer.ActionTYPE AS Action
  FROM `staging.customer_management` 
WHERE Customer.ActionType in ("NEW","UPDCUST" ,"INACT")
)
SELECT 
LAST_VALUE(temp_customer.CustomerID IGNORE NULLS) OVER (customer_updates) CustomerID,
LAST_VALUE(temp_customer.TaxID IGNORE NULLS) OVER (customer_updates) TaxID,
LAST_VALUE(temp_customer.Status IGNORE NULLS) OVER (customer_updates) Status,
LAST_VALUE(temp_customer.LastName IGNORE NULLS) OVER (customer_updates) LastName,
LAST_VALUE(temp_customer.FirstName IGNORE NULLS) OVER (customer_updates) FirstName,
LAST_VALUE(temp_customer.MiddleInitial IGNORE NULLS) OVER (customer_updates) MiddleInitial,
LAST_VALUE(temp_customer.Gender IGNORE NULLS) OVER (customer_updates) Gender,
LAST_VALUE(temp_customer.Tier IGNORE NULLS) OVER (customer_updates) Tier,
LAST_VALUE(temp_customer.DOB IGNORE NULLS) OVER (customer_updates) DOB,
LAST_VALUE(temp_customer.AddressLine1 IGNORE NULLS) OVER (customer_updates) AddressLine1,
LAST_VALUE(temp_customer.AddressLine2 IGNORE NULLS) OVER (customer_updates) AddressLine2,
LAST_VALUE(temp_customer.PostalCode IGNORE NULLS) OVER (customer_updates) PostalCode,
LAST_VALUE(temp_customer.City IGNORE NULLS) OVER (customer_updates) City,
LAST_VALUE(temp_customer.State_Prov IGNORE NULLS) OVER (customer_updates) State_Prov,
LAST_VALUE(temp_customer.Country IGNORE NULLS) OVER (customer_updates) Country,
LAST_VALUE(temp_customer.Phone1 IGNORE NULLS) OVER (customer_updates) Phone1,
LAST_VALUE(temp_customer.Phone2 IGNORE NULLS) OVER (customer_updates) Phone2,
LAST_VALUE(temp_customer.Phone3 IGNORE NULLS) OVER (customer_updates) Phone3,
LAST_VALUE(temp_customer.Email1 IGNORE NULLS) OVER (customer_updates) Email1,
LAST_VALUE(temp_customer.Email2 IGNORE NULLS) OVER (customer_updates) Email2,
LAST_VALUE(temp_customer.NationalTaxID IGNORE NULLS) OVER (customer_updates) NationalTaxID,
LAST_VALUE(temp_customer.LocalTaxID IGNORE NULLS) OVER (customer_updates) LocalTaxID,
Action,
DATE(effective_timestamp) AS EffectiveDate,
LEAD(DATE (effective_timestamp), 1, DATE ('9999-12-31')) OVER (customer_updates) AS EndDate
 FROM temp_customer
WINDOW customer_updates AS 
(PARTITION BY temp_customer.CustomerID
ORDER BY temp_customer.effective_timestamp)
;