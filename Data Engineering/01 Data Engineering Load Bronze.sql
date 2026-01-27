-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Bronze layer
-- MAGIC
-- MAGIC ## Connecting to the bronze layer (Target)

-- COMMAND ----------

USE CATALOG jeromeaymon_lakehouse;
USE DATABASE bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load data into bronze layer of the Lakehouse

-- COMMAND ----------

CREATE OR REPLACE TABLE address 
AS SELECT * FROM jay_adventureworks.saleslt.address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderDetail

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderDetail
AS SELECT * FROM jay_adventureworks.saleslt.SalesOrderDetail;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderHeader

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderHeader
AS SELECT * FROM jay_adventureworks.saleslt.SalesOrderHeader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Product

-- COMMAND ----------

CREATE OR REPLACE TABLE Product
AS SELECT * FROM jay_adventureworks.saleslt.Product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductCategory

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductCategory
AS SELECT * FROM jay_adventureworks.saleslt.ProductCategory;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Address
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE Address 
AS SELECT * FROM jay_adventureworks.saleslt.Address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Customer

-- COMMAND ----------

CREATE OR REPLACE TABLE Customer 
AS SELECT * FROM jay_adventureworks.saleslt.Customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion CustomerAddress

-- COMMAND ----------

CREATE OR REPLACE TABLE CustomerAddress
AS SELECT * FROM jay_adventureworks.saleslt.CustomerAddress;

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %md
-- MAGIC Ingestion ProductDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductDescription
AS SELECT * FROM jay_adventureworks.saleslt.ProductDescription;


-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC Ingestion ProductModel
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModel
AS SELECT * FROM jay_adventureworks.saleslt.ProductModel;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion ProductModelProductDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModelProductDescription
AS SELECT * FROM jay_adventureworks.saleslt.ProductModelProductDescription;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion vGetAllCategories
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE vGetAllCategories
AS SELECT * FROM jay_adventureworks.saleslt.vGetAllCategories;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion vProductAndDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE vProductAndDescription
AS SELECT * FROM jay_adventureworks.saleslt.vProductAndDescription;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ingestion vProductModelCatalogDescription
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE vProductModelCatalogDescription
AS SELECT * FROM jay_adventureworks.saleslt.vProductModelCatalogDescription;

-- COMMAND ----------

CREATE OR REPLACE TABLE jeromeaymon_lakehouse.bronze.sales_order_detail
AS SELECT * FROM jay_adventureworks.saleslt.salesorderdetail;
