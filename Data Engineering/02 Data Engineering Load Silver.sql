-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Silver layer
-- MAGIC
-- MAGIC ## Connecting to the Silver layer (Target)

-- COMMAND ----------

USE CATALOG jeromeaymon_lakehouse;
USE DATABASE silver;

-- COMMAND ----------

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of address

-- COMMAND ----------

-- DBTITLE 1,Cell 5
-- Étape 1: Créer une vue source avec hash
CREATE OR REPLACE TEMP VIEW _src_address AS
SELECT
  AddressID AS address_id,
  AddressLine1 AS address_line1,
  AddressLine2 AS address_line2,
  City AS city,
  StateProvince AS state_province,
  CountryRegion AS country_region,
  PostalCode AS postal_code,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  -- Hash pour détecter les changements
  MD5(CONCAT_WS('|',
    COALESCE(AddressLine1, ''),
    COALESCE(AddressLine2, ''),
    COALESCE(City, ''),
    COALESCE(StateProvince, ''),
    COALESCE(CountryRegion, ''),
    COALESCE(PostalCode, ''),
    COALESCE(rowguid, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash 
FROM bronze.address;

-- Étape 2: Fermer les enregistrements modifiés ou supprimés
MERGE INTO silver.address AS tgt
USING _src_address AS src
ON tgt.address_id = src.address_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  -- Le record a changé: fermer l'ancienne version
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  -- Le record n'existe plus dans la source: le marquer comme supprimé
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';





-- COMMAND ----------

-- Étape 3: Insérer les nouveaux enregistrements et les nouvelles versions
INSERT INTO silver.address (
  address_id, address_line1, address_line2, city, state_province,
  country_region, postal_code, rowguid, modified_date, _tf_row_hash,
  _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date, _tf_change_type
)
SELECT
  src.address_id,
  src.address_line1,
  src.address_line2,
  src.city,
  src.state_province,
  src.country_region,
  src.postal_code,
  src.rowguid,
  src.modified_date,
  src.row_hash,
  load_date AS _tf_valid_from,
  NULL AS _tf_valid_to,
  load_date AS _tf_create_date,
  load_date AS _tf_update_date,
  CASE
    WHEN NOT EXISTS (
      SELECT 1 FROM silver.address tgt
      WHERE tgt.address_id = src.address_id
    ) THEN 'INSERT'
    ELSE 'UPDATE'
  END AS _tf_change_type
FROM _src_address src
WHERE NOT EXISTS (
  -- N'insérer que si aucune version courante identique n'existe
  SELECT 1 FROM silver.address tgt
  WHERE tgt.address_id = src.address_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of customer 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_customer AS
SELECT
  CustomerID AS customer_id,
  NameStyle AS name_style,
  Title AS title,
  FirstName AS first_name,
  MiddleName AS middle_name,
  LastName AS last_name,
  Suffix AS suffix,
  CompanyName AS company_name,
  SalesPerson AS sales_person,
  EmailAddress AS email_address,
  Phone AS phone,
  PasswordHash AS password_hash,
  PasswordSalt AS password_salt,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(CAST(NameStyle AS STRING), ''),
    COALESCE(Title, ''),
    COALESCE(FirstName, ''),
    COALESCE(MiddleName, ''),
    COALESCE(LastName, ''),
    COALESCE(Suffix, ''),
    COALESCE(CompanyName, ''),
    COALESCE(SalesPerson, ''),
    COALESCE(EmailAddress, ''),
    COALESCE(Phone, ''),
    COALESCE(rowguid, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.customer;

-- Fermer les versions modifiées/supprimées
MERGE INTO silver.customer AS tgt
USING _src_customer AS src
ON tgt.customer_id = src.customer_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';


-- COMMAND ----------

-- Insérer nouvelles versions
INSERT INTO silver.customer (
  customer_id, 
  name_style, 
  title, 
  first_name, 
  middle_name, 
  last_name,
  suffix, 
  company_name, 
  sales_person, 
  email_address, 
  phone,
  password_hash, 
  password_salt, 
  rowguid, 
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.customer_id, 
  src.name_style,
  src.title, 
  src.first_name, 
  src.middle_name, 
  src.last_name,
  src.suffix, 
  src.company_name, 
  src.sales_person, 
  src.email_address, 
  src.phone,
  src.password_hash, 
  src.password_salt, 
  src.rowguid, 
  src.modified_date,
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to, 
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.customer tgt WHERE tgt.customer_id = src.customer_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_customer src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.customer tgt
  WHERE tgt.customer_id = src.customer_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of customeraddress

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_customeraddress AS
SELECT
  CustomerID AS customer_id,
  AddressID AS address_id,
  AddressType AS address_type,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(CAST(CustomerID AS STRING), ''),
    COALESCE(CAST(AddressID AS STRING), ''),
    COALESCE(AddressType, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.customeraddress;

-- COMMAND ----------

MERGE INTO silver.customeraddress AS tgt
USING _src_customeraddress AS src
ON tgt.customer_id = src.customer_id
  AND tgt.address_id = src.address_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.customeraddress (
  customer_id, 
  address_id, 
  address_type, 
  rowguid,
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.customer_id, 
  src.address_id, 
  src.address_type, 
  src.rowguid, 
  src.modified_date, 
  src.row_hash,
  load_date, 
  NULL, 
  load_date, 
  load_date,
  CASE
    WHEN NOT EXISTS (
      SELECT 1 FROM silver.customeraddress tgt 
      WHERE tgt.customer_id = src.customer_id AND tgt.address_id = src.address_id
    ) THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_customeraddress src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.customeraddress tgt
  WHERE tgt.customer_id = src.customer_id
    AND tgt.address_id = src.address_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of sales_order_header

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_sales_order_header AS
SELECT
  SalesOrderID AS sales_order_id,
  RevisionNumber AS revision_number,
  OrderDate AS order_date,
  DueDate AS due_date,
  ShipDate AS ship_date,
  Status AS status,
  OnlineOrderFlag AS online_order_flag,
  SalesOrderNumber AS sales_order_number,
  PurchaseOrderNumber AS purchase_order_number,
  AccountNumber AS account_number,
  CustomerID AS customer_id,
  ShipToAddressID AS ship_to_address_id,
  BillToAddressID AS bill_to_address_id,
  ShipMethod AS ship_method,
  CreditCardApprovalCode AS credit_card_approval_code,
  SubTotal AS sub_total,
  TaxAmt AS tax_amt,
  Freight AS freight,
  TotalDue AS total_due,
  Comment AS comment,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(CAST(RevisionNumber AS STRING), ''),
    COALESCE(CAST(OrderDate AS STRING), ''),
    COALESCE(CAST(Status AS STRING), ''),
    COALESCE(CAST(CustomerID AS STRING), ''),
    COALESCE(CAST(SubTotal AS STRING), ''),
    COALESCE(CAST(TaxAmt AS STRING), ''),
    COALESCE(CAST(Freight AS STRING), ''),
    COALESCE(CAST(TotalDue AS STRING), ''),
    COALESCE(CAST(rowguid AS STRING), ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.salesorderheader;

MERGE INTO silver.sales_order_header AS tgt
USING _src_sales_order_header AS src
ON tgt.sales_order_id = src.sales_order_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.sales_order_header
  (
    sales_order_id,
    revision_number,
    order_date,
    due_date,
    ship_date,
    status,
    online_order_flag,
    sales_order_number,
    purchase_order_number,
    account_number,
    customer_id,
    ship_to_address_id,
    bill_to_address_id,
    ship_method,
    credit_card_approval_code,
    sub_total,
    tax_amt,
    freight,
    total_due,
    comment,
    rowguid,
    modified_date,
    _tf_row_hash,
    _tf_valid_from,
    _tf_valid_to,
    _tf_create_date,
    _tf_update_date,
    _tf_change_type
  )
SELECT
    src.sales_order_id,
    src.revision_number,
    src.order_date,
    src.due_date,
    src.ship_date,
    src.status,
    src.online_order_flag,
    src.sales_order_number,
    src.purchase_order_number,
    src.account_number,
    src.customer_id,
    src.ship_to_address_id,
    src.bill_to_address_id,
    src.ship_method,
    src.credit_card_approval_code,
    src.sub_total,
    src.tax_amt,
    src.freight,
    src.total_due,
    src.comment,
    src.rowguid,
    src.modified_date,
    src.row_hash,
    load_date as _tf_valid_from,
    NULL as _tf_valid_to,
    load_date as _tf_create_date,
    load_date as _tf_update_date,
    CASE WHEN NOT EXISTS
      (
    SELECT 1
    FROM silver.sales_order_header tgt
    WHERE tgt.sales_order_id = src.sales_order_id) THEN
        'INSERT'
      ELSE
      'UPDATE'
      END
FROM
        _src_sales_order_header src
WHERE NOT EXISTS
      (SELECT 1
      FROM silver.sales_order_header tgt
      WHERE tgt.sales_order_id = src.sales_order_id
      AND     tgt._tf_is_current = TRUE
      AND     tgt._tf_row_hash   = src.row_hash );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental load of sales_order_detail

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_sales_order_detail AS
SELECT
  SalesOrderID AS sales_order_id,
  SalesOrderDetailID AS sales_order_detail_id,
  OrderQty AS order_qty,
  ProductID AS product_id,
  UnitPrice AS unit_price,
  UnitPriceDiscount AS unit_price_discount,
  LineTotal AS line_total,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(CAST(OrderQty AS STRING), ''),
    COALESCE(CAST(ProductID AS STRING), ''),
    COALESCE(CAST(UnitPrice AS STRING), ''),
    COALESCE(CAST(UnitPriceDiscount AS STRING), ''),
    COALESCE(CAST(LineTotal AS STRING), ''),
    COALESCE(CAST(rowguid AS STRING), ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.salesorderdetail;

-- COMMAND ----------

MERGE INTO silver.sales_order_detail AS tgt
USING _src_sales_order_detail AS src
ON tgt.sales_order_id = src.sales_order_id
  AND tgt.sales_order_detail_id = src.sales_order_detail_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.sales_order_detail (
  sales_order_id, 
  sales_order_detail_id, 
  order_qty, 
  product_id,
  unit_price, 
  unit_price_discount, 
  line_total, 
  rowguid, 
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date,
  _tf_change_type
)
SELECT
  src.sales_order_id, 
  src.sales_order_detail_id, 
  src.order_qty, 
  src.product_id,
  src.unit_price, 
  src.unit_price_discount, 
  src.line_total, 
  src.rowguid, 
  src.modified_date, 
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to, 
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (
      SELECT 1 FROM silver.sales_order_detail tgt
      WHERE tgt.sales_order_id = src.sales_order_id
        AND tgt.sales_order_detail_id = src.sales_order_detail_id
    ) THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_sales_order_detail src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.sales_order_detail tgt
  WHERE tgt.sales_order_id = src.sales_order_id
    AND tgt.sales_order_detail_id = src.sales_order_detail_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load Product with scd type 2

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_product AS
SELECT
  ProductID AS product_id,
  Name AS name,
  ProductNumber AS product_number,
  Color AS color,
  StandardCost AS standard_cost,
  ListPrice AS list_price,
  Size AS size,
  Weight AS weight,
  ProductCategoryID AS product_category_id,
  ProductModelID AS product_model_id,
  SellStartDate AS sell_start_date,
  SellEndDate AS sell_end_date,
  DiscontinuedDate AS discontinued_date,
  ThumbNailPhoto AS thumbnail_photo,
  ThumbnailPhotoFileName AS thumbnail_photo_file_name,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(Name, ''),
    COALESCE(ProductNumber, ''),
    COALESCE(Color, ''),
    COALESCE(CAST(StandardCost AS STRING), ''),
    COALESCE(CAST(ListPrice AS STRING), ''),
    COALESCE(Size, ''),
    COALESCE(CAST(Weight AS STRING), ''),
    COALESCE(CAST(ProductCategoryID AS STRING), ''),
    COALESCE(CAST(ProductModelID AS STRING), ''),
    COALESCE(CAST(SellStartDate AS STRING), ''),
    COALESCE(CAST(SellEndDate AS STRING), ''),
    COALESCE(CAST(DiscontinuedDate AS STRING), ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.product;

MERGE INTO silver.product AS tgt
USING _src_product AS src
ON tgt.product_id = src.product_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.product (
  product_id, 
  name, 
  product_number, 
  color, 
  standard_cost, 
  list_price, 
  size, 
  weight,
  product_category_id, 
  product_model_id, 
  sell_start_date, 
  sell_end_date, 
  discontinued_date,
  thumbnail_photo, 
  thumbnail_photo_file_name, 
  rowguid, 
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date,
   _tf_change_type
)
SELECT
  src.product_id, 
  src.name, 
  src.product_number, 
  src.color, 
  src.standard_cost, 
  src.list_price, 
  src.size, 
  src.weight, 
  src.product_category_id, 
  src.product_model_id, 
  src.sell_start_date, 
  src.sell_end_date, 
  src.discontinued_date, 
  src.thumbnail_photo, 
  src.thumbnail_photo_file_name, 
  src.rowguid, 
  src.modified_date, 
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to, 
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.product tgt WHERE tgt.product_id = src.product_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_product src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.product tgt
  WHERE tgt.product_id = src.product_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Load productcategory 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_productcategory AS
SELECT
  ProductCategoryID AS product_category_id,
  ParentProductCategoryID AS parent_product_category_id,
  Name AS name,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(CAST(ParentProductCategoryID AS STRING), ''),
    COALESCE(Name, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.productcategory;

MERGE INTO silver.productcategory AS tgt
USING _src_productcategory AS src
ON tgt.product_category_id = src.product_category_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.productcategory (
  product_category_id, 
  parent_product_category_id, 
  name, 
  rowguid, 
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.product_category_id, 
  src.parent_product_category_id, 
  src.name, 
  src.rowguid, 
  src.modified_date, 
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to, 
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.productcategory tgt WHERE tgt.product_category_id = src.product_category_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_productcategory src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.productcategory tgt
  WHERE tgt.product_category_id = src.product_category_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load productdescription

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_productdescription AS
SELECT
  ProductDescriptionID AS product_description_id,
  Description AS description,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(Description, ''),
    COALESCE(CAST(ModifiedDate AS STRING), ''),
    COALESCE(CAST(rowguid AS STRING), '')
  )) AS row_hash
FROM bronze.productdescription;


MERGE INTO silver.productdescription AS tgt
USING _src_productdescription AS src
ON tgt.product_description_id = src.product_description_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.productdescription (
  product_description_id, 
  description, 
  rowguid, 
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.product_description_id, src.description, src.rowguid, src.modified_date, src.row_hash,
  load_date, NULL, load_date, load_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.productdescription tgt WHERE tgt.product_description_id = src.product_description_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_productdescription src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.productdescription tgt
  WHERE tgt.product_description_id = src.product_description_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load productmodel

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_productmodel AS
SELECT
  ProductModelID AS product_model_id,
  Name AS name,
  CatalogDescription AS catalog_description,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(Name, ''),
    COALESCE(CatalogDescription, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.productmodel;

-- COMMAND ----------

MERGE INTO silver.productmodel AS tgt
USING _src_productmodel AS src
ON tgt.product_model_id = src.product_model_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.productmodel (
  product_model_id, 
  name, 
  catalog_description, 
  rowguid, 
  modified_date, 
  _tf_row_hash,
  _tf_valid_from, 
  
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.product_model_id, 
  src.name, 
  src.catalog_description, 
  src.rowguid, 
  src.modified_date, 
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to, 
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.productmodel tgt WHERE tgt.product_model_id = src.product_model_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_productmodel src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.productmodel tgt
  WHERE tgt.product_model_id = src.product_model_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load productmodelproductdescription

-- COMMAND ----------

-- DBTITLE 1,Insert new productmodelproductdescription records
CREATE OR REPLACE TEMP VIEW _src_productmodelproductdescription AS
SELECT
  ProductModelID AS product_model_id,
  ProductDescriptionID AS product_description_id,
  Culture AS culture,
  rowguid AS rowguid,
  ModifiedDate AS modified_date,
  MD5(CONCAT_WS('|',
    COALESCE(CAST(ProductModelID AS STRING), ''),
    COALESCE(CAST(ProductDescriptionID AS STRING), ''),
    COALESCE(Culture, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.productmodelproductdescription;

-- COMMAND ----------

MERGE INTO silver.productmodelproductdescription AS tgt
USING _src_productmodelproductdescription AS src
ON tgt.product_model_id = src.product_model_id
  AND tgt.product_description_id = src.product_description_id
  AND tgt.culture = src.culture
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------


INSERT INTO silver.productmodelproductdescription (
  product_model_id, 
  product_description_id, 
  culture, 
  rowguid,
   modified_date, 
   _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.product_model_id, 
  src.product_description_id, 
  src.culture, 
  src.rowguid, 
  src.modified_date, 
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to,  
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (
      SELECT 1 FROM silver.productmodelproductdescription tgt 
      WHERE tgt.product_model_id = src.product_model_id 
        AND tgt.product_description_id = src.product_description_id
        AND tgt.culture = src.culture
    ) THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_productmodelproductdescription src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.productmodelproductdescription tgt
  WHERE tgt.product_model_id = src.product_model_id
    AND tgt.product_description_id = src.product_description_id
    AND tgt.culture = src.culture
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load vgetallcategories

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_vgetallcategories AS
SELECT
  ProductCategoryID AS product_category_id,
  ParentProductCategoryName AS parent_product_category_name,
  ProductCategoryName AS name,
  MD5(CONCAT_WS('|',
    COALESCE(ParentProductCategoryName, ''),
    COALESCE(ProductCategoryName, '')
  )) AS row_hash
FROM bronze.vgetallcategories;

-- COMMAND ----------

MERGE INTO silver.vgetallcategories AS tgt
USING _src_vgetallcategories AS src
ON tgt.product_category_id = src.product_category_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

-- DBTITLE 1,Insert new vgetallcategories records
INSERT INTO silver.vgetallcategories (
  product_category_id, 
  parent_product_category_id, 
  name, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.product_category_id,
  (SELECT pc.ParentProductCategoryID 
   FROM bronze.productcategory pc 
   WHERE pc.ProductCategoryID = src.product_category_id 
   LIMIT 1) AS parent_product_category_id,
  src.name,
  src.row_hash,
  load_date as _tf_valid_from, 
  NULL as _tf_valid_to, 
  load_date as _tf_create_date, 
  load_date as _tf_update_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.vgetallcategories tgt WHERE tgt.product_category_id = src.product_category_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_vgetallcategories src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.vgetallcategories tgt
  WHERE tgt.product_category_id = src.product_category_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load vproductanddescription

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_vproductanddescription AS
SELECT
  ProductID AS product_id,
  Name AS name,
  ProductModel AS product_model,
  Culture AS culture,
  Description AS description,
  MD5(CONCAT_WS('|',
    COALESCE(Name, ''),
    COALESCE(ProductModel, ''),
    COALESCE(Culture, ''),
    COALESCE(Description, '')
  )) AS row_hash
FROM bronze.vproductanddescription;

-- COMMAND ----------

MERGE INTO silver.vproductanddescription AS tgt
USING _src_vproductanddescription AS src
ON tgt.product_id = src.product_id
  AND tgt.culture = src.culture
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';


-- COMMAND ----------

INSERT INTO silver.vproductanddescription (
  product_id, 
  name, 
  product_model, 
  culture, 
  description, 
  _tf_row_hash,
  _tf_valid_from, 
  _tf_valid_to, 
  _tf_create_date, 
  _tf_update_date, 
  _tf_change_type
)
SELECT
  src.product_id, 
  src.name, 
  src.product_model, 
  src.culture, 
  src.description, 
  src.row_hash,
  load_date, 
  NULL, 
  load_date, 
  load_date,
  CASE
    WHEN NOT EXISTS (
      SELECT 1 FROM silver.vproductanddescription tgt 
      WHERE tgt.product_id = src.product_id AND tgt.culture = src.culture
    ) THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_vproductanddescription src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.vproductanddescription tgt
  WHERE tgt.product_id = src.product_id
    AND tgt.culture = src.culture
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load vpproductmodelcatalogdescription

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_vproductmodelcatalogdescription AS
SELECT
  ProductModelID AS product_model_id,
  Name AS name,
  Summary AS summary,
  Manufacturer AS manufacturer,
  Copyright AS copyright,
  ProductURL AS producturl,
  WarrantyPeriod AS warrantyperiod,
  WarrantyDescription AS warrantydescription,
  NoOfYears AS noofyears,
  MaintenanceDescription AS maintenancedescription,
  Wheel AS wheel,
  Saddle AS saddle,
  Pedal AS pedal,
  BikeFrame AS bikeframe,
  Crankset AS crankset,
  PictureAngle AS pictureangle,
  PictureSize AS picturesize,
  ProductPhotoID AS productphotoid,
  Material AS material,
  Color AS color,
  ProductLine AS productline,
  Style AS style,
  RiderExperience AS riderexperience,
  rowguid AS rowguid,
  ModifiedDate AS modifieddate,
  MD5(CONCAT_WS('|',
    COALESCE(Name, ''),
    COALESCE(Summary, ''),
    COALESCE(Manufacturer, ''),
    COALESCE(CAST(ModifiedDate AS STRING), '')
  )) AS row_hash
FROM bronze.vproductmodelcatalogdescription;

-- COMMAND ----------

MERGE INTO silver.vproductmodelcatalogdescription AS tgt
USING _src_vproductmodelcatalogdescription AS src
ON tgt.product_model_id = src.product_model_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND tgt._tf_row_hash != src.row_hash THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'UPDATE'
WHEN NOT MATCHED BY SOURCE AND tgt._tf_is_current = TRUE THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date,
    tgt._tf_change_type = 'DELETE';

-- COMMAND ----------

INSERT INTO silver.vproductmodelcatalogdescription (
  product_model_id, name, summary, manufacturer, copyright, producturl,
  warrantyperiod, warrantydescription, noofyears, maintenancedescription,
  wheel, saddle, pedal, bikeframe, crankset, pictureangle, picturesize,
  productphotoid, material, color, productline, style, riderexperience,
  rowguid, modifieddate, _tf_row_hash,
  _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date, _tf_change_type
)
SELECT
  src.product_model_id, src.name, src.summary, src.manufacturer, src.copyright, src.producturl,
  src.warrantyperiod, src.warrantydescription, src.noofyears, src.maintenancedescription,
  src.wheel, src.saddle, src.pedal, src.bikeframe, src.crankset, src.pictureangle, src.picturesize,
  src.productphotoid, src.material, src.color, src.productline, src.style, src.riderexperience,
  src.rowguid, src.modifieddate, src.row_hash,
  load_date, NULL, load_date, load_date,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM silver.vproductmodelcatalogdescription tgt WHERE tgt.product_model_id = src.product_model_id)
    THEN 'INSERT'
    ELSE 'UPDATE'
  END
FROM _src_vproductmodelcatalogdescription src
WHERE NOT EXISTS (
  SELECT 1 FROM silver.vproductmodelcatalogdescription tgt
  WHERE tgt.product_model_id = src.product_model_id
    AND tgt._tf_is_current = TRUE
    AND tgt._tf_row_hash = src.row_hash
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contrôles de qualité Silver

-- COMMAND ----------

SELECT 
  'address' AS table_name,
  COUNT(*) AS total_records,
  SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END) AS current_records,
  SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) AS historical_records
FROM silver.address
UNION ALL
SELECT 'customer', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.customer
UNION ALL
SELECT 'customeraddress', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.customeraddress
UNION ALL
SELECT 'sales_order_header', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.sales_order_header
UNION ALL
SELECT 'sales_order_detail', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.sales_order_detail
UNION ALL
SELECT 'product', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.product
UNION ALL
SELECT 'productcategory', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.productcategory
UNION ALL
SELECT 'productdescription', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.productdescription
UNION ALL
SELECT 'productmodel', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.productmodel
UNION ALL
SELECT 'productmodelproductdescription', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.productmodelproductdescription
UNION ALL
SELECT 'vgetallcategories', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.vgetallcategories
UNION ALL
SELECT 'vproductanddescription', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.vproductanddescription
UNION ALL
SELECT 'vproductmodelcatalogdescription', COUNT(*), SUM(CASE WHEN _tf_is_current = TRUE THEN 1 ELSE 0 END), SUM(CASE WHEN _tf_is_current = FALSE THEN 1 ELSE 0 END) FROM silver.vproductmodelcatalogdescription
ORDER BY table_name;
