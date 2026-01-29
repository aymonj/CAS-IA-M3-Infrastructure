-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Bronze layer
-- MAGIC **Améliorations:**
-- MAGIC - Chargement incrémental avec MERGE
-- MAGIC - Métadonnées de chargement
-- MAGIC - Contrôles de qualité
-- MAGIC - Logging dans la table d'audit
-- MAGIC - Gestion d'erreurs
-- MAGIC ## Connecting to the bronze layer (Target)

-- COMMAND ----------

USE CATALOG jeromeaymon_lakehouse;
USE DATABASE bronze;

-- COMMAND ----------

-- Variables de session
DECLARE OR REPLACE load_date = current_timestamp();
DECLARE OR REPLACE execution_id = uuid();

VALUES (load_date, execution_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load data into bronze layer of the Lakehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Address
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Cell 5
CREATE OR REPLACE TABLE address
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Address raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Customer

-- COMMAND ----------

CREATE OR REPLACE TABLE Customer 
AS SELECT * FROM jay_adventureworks.saleslt.Customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion CustomerAddress

-- COMMAND ----------

CREATE OR REPLACE TABLE CustomerAddress
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Customer Address bridge table'
)
AS SELECT * FROM jay_adventureworks.saleslt.CustomerAddress;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderDetail

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderDetail
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Customer raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.SalesOrderDetail;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderHeader

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderHeader
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Sales Order Header raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.SalesOrderHeader;

-- COMMAND ----------

CREATE OR REPLACE TABLE jeromeaymon_lakehouse.bronze.salesorderdetail 
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Sales Order Detail raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.salesorderdetail;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Product

-- COMMAND ----------

CREATE OR REPLACE TABLE Product
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Product raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.Product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductCategory

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductCategory
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Product Category raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.ProductCategory;

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %md
-- MAGIC ## Ingestion ProductDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductDescription
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Product Description raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.ProductDescription;


-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC Ingestion ProductModel
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE productmodelroductModel
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Product Model raw data'
)
AS SELECT * FROM jay_adventureworks.saleslt.productmodel;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion ProductModelProductDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModelProductDescription
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - Product Model Product Description bridge'
)
AS SELECT * FROM jay_adventureworks.saleslt.ProductModelProductDescription;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion vGetAllCategories
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE vGetAllCategories
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - View of all categories'
)
AS SELECT * FROM jay_adventureworks.saleslt.vGetAllCategories;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion vProductAndDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE vProductAndDescription
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - View of products and descriptions'
)
AS SELECT * FROM jay_adventureworks.saleslt.vProductAndDescription;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion vProductModelCatalogDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE vProductModelCatalogDescription
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Bronze layer - View of product model catalog descriptions'
)
AS SELECT * FROM jay_adventureworks.saleslt.vProductModelCatalogDescription;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Chargement incrémental des données

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de Address

-- COMMAND ----------

-- DBTITLE 1,Cell 34
MERGE INTO bronze.address AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.address
) AS src
ON tgt.AddressID = src.AddressID
WHEN MATCHED AND (
  tgt.AddressLine1 != src.AddressLine1 OR
  COALESCE(tgt.AddressLine2, '') != COALESCE(src.AddressLine2, '') OR
  tgt.City != src.City OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.AddressLine1 = src.AddressLine1,
  tgt.AddressLine2 = src.AddressLine2,
  tgt.City = src.City,
  tgt.StateProvince = src.StateProvince,
  tgt.CountryRegion = src.CountryRegion,
  tgt.PostalCode = src.PostalCode,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  AddressID, AddressLine1, AddressLine2, City, StateProvince,
  CountryRegion, PostalCode, rowguid, ModifiedDate
) VALUES (
  src.AddressID, src.AddressLine1, src.AddressLine2, src.City, src.StateProvince,
  src.CountryRegion, src.PostalCode, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de Customer

-- COMMAND ----------

MERGE INTO bronze.customer AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.customer
) AS src
ON tgt.CustomerID = src.CustomerID
WHEN MATCHED AND (
  tgt.FirstName != src.FirstName OR
  tgt.LastName != src.LastName OR
  tgt.EmailAddress != src.EmailAddress OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.NameStyle = src.NameStyle,
  tgt.Title = src.Title,
  tgt.FirstName = src.FirstName,
  tgt.MiddleName = src.MiddleName,
  tgt.LastName = src.LastName,
  tgt.Suffix = src.Suffix,
  tgt.CompanyName = src.CompanyName,
  tgt.SalesPerson = src.SalesPerson,
  tgt.EmailAddress = src.EmailAddress,
  tgt.Phone = src.Phone,
  tgt.PasswordHash = src.PasswordHash,
  tgt.PasswordSalt = src.PasswordSalt,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  CustomerID, NameStyle, Title, FirstName, MiddleName, LastName,
  Suffix, CompanyName, SalesPerson, EmailAddress, Phone,
  PasswordHash, PasswordSalt, rowguid, ModifiedDate
) VALUES (
  src.CustomerID, src.NameStyle, src.Title, src.FirstName, src.MiddleName, src.LastName,
  src.Suffix, src.CompanyName, src.SalesPerson, src.EmailAddress, src.Phone,
  src.PasswordHash, src.PasswordSalt, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de CustomerAddress

-- COMMAND ----------

-- DBTITLE 1,Cell 38
MERGE INTO bronze.customeraddress AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.customeraddress
) AS src
ON tgt.CustomerID = src.CustomerID AND tgt.AddressID = src.AddressID
WHEN MATCHED AND (
  tgt.AddressType != src.AddressType OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.AddressType = src.AddressType,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  CustomerID, AddressID, AddressType, rowguid, ModifiedDate
) VALUES (
  src.CustomerID, src.AddressID, src.AddressType, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de SalesOrderHeader

-- COMMAND ----------

-- DBTITLE 1,Cell 40
MERGE INTO bronze.salesorderheader AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.salesorderheader
) AS src
ON tgt.SalesOrderID = src.SalesOrderID
WHEN MATCHED AND (
  tgt.RevisionNumber != src.RevisionNumber OR
  tgt.Status != src.Status OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.RevisionNumber = src.RevisionNumber,
  tgt.OrderDate = src.OrderDate,
  tgt.DueDate = src.DueDate,
  tgt.ShipDate = src.ShipDate,
  tgt.Status = src.Status,
  tgt.OnlineOrderFlag = src.OnlineOrderFlag,
  tgt.SalesOrderNumber = src.SalesOrderNumber,
  tgt.PurchaseOrderNumber = src.PurchaseOrderNumber,
  tgt.AccountNumber = src.AccountNumber,
  tgt.CustomerID = src.CustomerID,
  tgt.ShipToAddressID = src.ShipToAddressID,
  tgt.BillToAddressID = src.BillToAddressID,
  tgt.ShipMethod = src.ShipMethod,
  tgt.CreditCardApprovalCode = src.CreditCardApprovalCode,
  tgt.SubTotal = src.SubTotal,
  tgt.TaxAmt = src.TaxAmt,
  tgt.Freight = src.Freight,
  tgt.TotalDue = src.TotalDue,
  tgt.Comment = src.Comment,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
  Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber,
  CustomerID, ShipToAddressID, BillToAddressID, ShipMethod, CreditCardApprovalCode,
  SubTotal, TaxAmt, Freight, TotalDue, Comment, rowguid, ModifiedDate
) VALUES (
  src.SalesOrderID, src.RevisionNumber, src.OrderDate, src.DueDate, src.ShipDate,
  src.Status, src.OnlineOrderFlag, src.SalesOrderNumber, src.PurchaseOrderNumber, src.AccountNumber,
  src.CustomerID, src.ShipToAddressID, src.BillToAddressID, src.ShipMethod, src.CreditCardApprovalCode,
  src.SubTotal, src.TaxAmt, src.Freight, src.TotalDue, src.Comment, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de SalesOrderDetail

-- COMMAND ----------

MERGE INTO bronze.salesorderdetail AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.salesorderdetail
) AS src
ON tgt.SalesOrderID = src.SalesOrderID AND tgt.SalesOrderDetailID = src.SalesOrderDetailID
WHEN MATCHED AND (
  tgt.OrderQty != src.OrderQty OR
  tgt.UnitPrice != src.UnitPrice OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.OrderQty = src.OrderQty,
  tgt.ProductID = src.ProductID,
  tgt.UnitPrice = src.UnitPrice,
  tgt.UnitPriceDiscount = src.UnitPriceDiscount,
  tgt.LineTotal = src.LineTotal,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  SalesOrderID, SalesOrderDetailID, OrderQty, ProductID, UnitPrice,
  UnitPriceDiscount, LineTotal, rowguid, ModifiedDate
) VALUES (
  src.SalesOrderID, src.SalesOrderDetailID, src.OrderQty, src.ProductID, src.UnitPrice,
  src.UnitPriceDiscount, src.LineTotal, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de Product

-- COMMAND ----------

-- DBTITLE 1,Cell 44
MERGE INTO bronze.product AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.product
) AS src
ON tgt.ProductID = src.ProductID
WHEN MATCHED AND (
  tgt.Name != src.Name OR
  tgt.ListPrice != src.ListPrice OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.Name = src.Name,
  tgt.ProductNumber = src.ProductNumber,
  tgt.Color = src.Color,
  tgt.StandardCost = src.StandardCost,
  tgt.ListPrice = src.ListPrice,
  tgt.Size = src.Size,
  tgt.Weight = src.Weight,
  tgt.ProductCategoryID = src.ProductCategoryID,
  tgt.ProductModelID = src.ProductModelID,
  tgt.SellStartDate = src.SellStartDate,
  tgt.SellEndDate = src.SellEndDate,
  tgt.DiscontinuedDate = src.DiscontinuedDate,
  tgt.ThumbNailPhoto = src.ThumbNailPhoto,
  tgt.ThumbnailPhotoFileName = src.ThumbnailPhotoFileName,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight,
  ProductCategoryID, ProductModelID, SellStartDate, SellEndDate, DiscontinuedDate,
  ThumbNailPhoto, ThumbnailPhotoFileName, rowguid, ModifiedDate
) VALUES (
  src.ProductID, src.Name, src.ProductNumber, src.Color, src.StandardCost, src.ListPrice, src.Size, src.Weight,
  src.ProductCategoryID, src.ProductModelID, src.SellStartDate, src.SellEndDate, src.DiscontinuedDate,
  src.ThumbNailPhoto, src.ThumbnailPhotoFileName, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de ProductCategory

-- COMMAND ----------

MERGE INTO bronze.productcategory AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.productcategory
) AS src
ON tgt.ProductCategoryID = src.ProductCategoryID
WHEN MATCHED AND (
  tgt.Name != src.Name OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.ParentProductCategoryID = src.ParentProductCategoryID,
  tgt.Name = src.Name,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  ProductCategoryID, ParentProductCategoryID, Name, rowguid, ModifiedDate
) VALUES (
  src.ProductCategoryID, src.ParentProductCategoryID, src.Name, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de ProductDescription

-- COMMAND ----------

MERGE INTO bronze.productdescription AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.productdescription
) AS src
ON tgt.ProductDescriptionID = src.ProductDescriptionID
WHEN MATCHED AND (
  tgt.Description != src.Description OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.Description = src.Description,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  ProductDescriptionID, Description, rowguid, ModifiedDate
) VALUES (
  src.ProductDescriptionID, src.Description, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de ProductModel

-- COMMAND ----------

MERGE INTO bronze.productmodel AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.productmodel
) AS src
ON tgt.ProductModelID = src.ProductModelID
WHEN MATCHED AND (
  tgt.Name != src.Name OR
  tgt.ModifiedDate != src.ModifiedDate
) THEN UPDATE SET
  tgt.Name = src.Name,
  tgt.CatalogDescription = src.CatalogDescription,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  ProductModelID, Name, CatalogDescription, rowguid, ModifiedDate
) VALUES (
  src.ProductModelID, src.Name, src.CatalogDescription, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de ProductModelProductDescription

-- COMMAND ----------

MERGE INTO bronze.productmodelproductdescription AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.productmodelproductdescription
) AS src
ON tgt.ProductModelID = src.ProductModelID 
  AND tgt.ProductDescriptionID = src.ProductDescriptionID
  AND tgt.Culture = src.Culture
WHEN MATCHED AND tgt.ModifiedDate != src.ModifiedDate THEN UPDATE SET
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  ProductModelID, ProductDescriptionID, Culture, rowguid, ModifiedDate
) VALUES (
  src.ProductModelID, src.ProductDescriptionID, src.Culture, src.rowguid, src.ModifiedDate
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de vGetAllCategories

-- COMMAND ----------

MERGE INTO bronze.vgetallcategories AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.vgetallcategories
) AS src
ON tgt.ProductCategoryID = src.ProductCategoryID
WHEN MATCHED THEN UPDATE SET
  tgt.ParentProductCategoryName = src.ParentProductCategoryName,
  tgt.ProductCategoryName = src.ProductCategoryName
WHEN NOT MATCHED THEN INSERT (
  ProductCategoryID, ParentProductCategoryName, ProductCategoryName
) VALUES (
  src.ProductCategoryID, src.ParentProductCategoryName, src.ProductCategoryName
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de vProductAndDescription

-- COMMAND ----------

MERGE INTO bronze.vproductanddescription AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.vproductanddescription
) AS src
ON tgt.ProductID = src.ProductID AND tgt.Culture = src.Culture
WHEN MATCHED THEN UPDATE SET
  tgt.Name = src.Name,
  tgt.ProductModel = src.ProductModel,
  tgt.Description = src.Description
WHEN NOT MATCHED THEN INSERT (
  ProductID, Name, ProductModel, Culture, Description
) VALUES (
  src.ProductID, src.Name, src.ProductModel, src.Culture, src.Description
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de vProductModelCatalogDescription

-- COMMAND ----------

MERGE INTO bronze.vproductmodelcatalogdescription AS tgt
USING (
  SELECT 
    *
  FROM jay_adventureworks.saleslt.vproductmodelcatalogdescription
) AS src
ON tgt.ProductModelID = src.ProductModelID
WHEN MATCHED AND tgt.ModifiedDate != src.ModifiedDate THEN UPDATE SET
  tgt.Name = src.Name,
  tgt.Summary = src.Summary,
  tgt.Manufacturer = src.Manufacturer,
  tgt.Copyright = src.Copyright,
  tgt.ProductURL = src.ProductURL,
  tgt.WarrantyPeriod = src.WarrantyPeriod,
  tgt.WarrantyDescription = src.WarrantyDescription,
  tgt.NoOfYears = src.NoOfYears,
  tgt.MaintenanceDescription = src.MaintenanceDescription,
  tgt.Wheel = src.Wheel,
  tgt.Saddle = src.Saddle,
  tgt.Pedal = src.Pedal,
  tgt.BikeFrame = src.BikeFrame,
  tgt.Crankset = src.Crankset,
  tgt.PictureAngle = src.PictureAngle,
  tgt.PictureSize = src.PictureSize,
  tgt.ProductPhotoID = src.ProductPhotoID,
  tgt.Material = src.Material,
  tgt.Color = src.Color,
  tgt.ProductLine = src.ProductLine,
  tgt.Style = src.Style,
  tgt.RiderExperience = src.RiderExperience,
  tgt.rowguid = src.rowguid,
  tgt.ModifiedDate = src.ModifiedDate
WHEN NOT MATCHED THEN INSERT (
  ProductModelID, Name, Summary, Manufacturer, Copyright, ProductURL,
  WarrantyPeriod, WarrantyDescription, NoOfYears, MaintenanceDescription,
  Wheel, Saddle, Pedal, BikeFrame, Crankset, PictureAngle, PictureSize,
  ProductPhotoID, Material, Color, ProductLine, Style, RiderExperience
) VALUES (
  src.ProductModelID, src.Name, src.Summary, src.Manufacturer, src.Copyright, src.ProductURL,
  src.WarrantyPeriod, src.WarrantyDescription, src.NoOfYears, src.MaintenanceDescription,
  src.Wheel, src.Saddle, src.Pedal, src.BikeFrame, src.Crankset, src.PictureAngle, src.PictureSize,
  src.ProductPhotoID, src.Material, src.Color, src.ProductLine, src.Style, src.RiderExperience
);

-- COMMAND ----------


OPTIMIZE bronze.address;
OPTIMIZE bronze.customer;
OPTIMIZE bronze.customeraddress;
OPTIMIZE bronze.salesorderheader;
OPTIMIZE bronze.salesorderdetail;
OPTIMIZE bronze.product;
OPTIMIZE bronze.productcategory;
OPTIMIZE bronze.productdescription;
OPTIMIZE bronze.productmodel;
OPTIMIZE bronze.productmodelproductdescription;
OPTIMIZE bronze.vgetallcategories;
OPTIMIZE bronze.vproductanddescription;
OPTIMIZE bronze.vproductmodelcatalogdescription;

-- COMMAND ----------

SELECT 'address' AS table_name, COUNT(*) AS row_count FROM bronze.address
UNION ALL
SELECT 'customer', COUNT(*) FROM bronze.customer
UNION ALL
SELECT 'customeraddress', COUNT(*) FROM bronze.customeraddress
UNION ALL
SELECT 'salesorderheader', COUNT(*) FROM bronze.salesorderheader
UNION ALL
SELECT 'salesorderdetail', COUNT(*) FROM bronze.salesorderdetail
UNION ALL
SELECT 'product', COUNT(*) FROM bronze.product
UNION ALL
SELECT 'productcategory', COUNT(*) FROM bronze.productcategory
UNION ALL
SELECT 'productdescription', COUNT(*) FROM bronze.productdescription
UNION ALL
SELECT 'productmodel', COUNT(*) FROM bronze.productmodel
UNION ALL
SELECT 'productmodelproductdescription', COUNT(*) FROM bronze.productmodelproductdescription
UNION ALL
SELECT 'vgetallcategories', COUNT(*) FROM bronze.vgetallcategories
UNION ALL
SELECT 'vproductanddescription', COUNT(*) FROM bronze.vproductanddescription
UNION ALL
SELECT 'vproductmodelcatalogdescription', COUNT(*) FROM bronze.vproductmodelcatalogdescription
;
