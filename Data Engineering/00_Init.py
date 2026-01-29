# Databricks notebook source
# MAGIC %md
# MAGIC # Creating the lakehouse catalog
# MAGIC  **Améliorations:**
# MAGIC - Change Data Feed activé pour le streaming
# MAGIC - Auto-optimisation Delta activée
# MAGIC - Contraintes de qualité des données
# MAGIC - Partitionnement pour les grandes tables
# MAGIC - Documentation enrichie
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backup et suppression sécurisée

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC -- Note: SET commands for spark.databricks.delta.properties.defaults are not supported in SQL warehouses
# MAGIC -- These properties are applied directly to each table using TBLPROPERTIES instead
# MAGIC -- See table definitions below for: enableChangeDataFeed, autoOptimize.optimizeWrite, autoOptimize.autoCompact

# COMMAND ----------

# MAGIC %md
# MAGIC ## WARNING !!! the following cell drops the existing lakehouse layers
# MAGIC Décommentez la cellule suivante pour effectuer le DROP

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE IF EXISTS jeromeaymon_lakehouse.bronze CASCADE;
# MAGIC -- DROP DATABASE IF EXISTS jeromeaymon_lakehouse.silver CASCADE; 
# MAGIC -- DROP DATABASE IF EXISTS jeromeaymon_lakehouse.gold CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the Medallion Architecture

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG jeromeaymon_lakehouse;
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS jeromeaymon_lakehouse.bronze COMMENT 'Bronze layer - Raw data ingestion';
# MAGIC CREATE DATABASE IF NOT EXISTS jeromeaymon_lakehouse.silver COMMENT 'Silver layer - Cleaned and historized data with SCD Type 2';
# MAGIC CREATE DATABASE IF NOT EXISTS jeromeaymon_lakehouse.gold COMMENT 'Gold layer - Business-level aggregates and dimensions';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Création de la table d'audit

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.audit.pipeline_execution (
# MAGIC   execution_id STRING NOT NULL,
# MAGIC   layer STRING NOT NULL,
# MAGIC   table_name STRING NOT NULL,
# MAGIC   start_time TIMESTAMP NOT NULL,
# MAGIC   end_time TIMESTAMP,
# MAGIC   records_processed BIGINT,
# MAGIC   records_inserted BIGINT,
# MAGIC   records_updated BIGINT,
# MAGIC   records_deleted BIGINT,
# MAGIC   status STRING NOT NULL,
# MAGIC   error_message STRING,
# MAGIC   load_date TIMESTAMP NOT NULL
# MAGIC )
# MAGIC PARTITIONED BY (DATE(start_time))
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'description' = 'Pipeline execution audit log'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating tables in Silver layer
# MAGIC
# MAGIC We add for each table an incremental surrogate key together with technical fields columns. 

# COMMAND ----------

# DBTITLE 1,Cell 11
# MAGIC %sql
# MAGIC     
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.address (
# MAGIC   _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC   
# MAGIC   -- Business key
# MAGIC   address_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Attributes
# MAGIC   address_line1 STRING,
# MAGIC   address_line2 STRING,
# MAGIC   city STRING,
# MAGIC   state_province STRING,
# MAGIC   country_region STRING,
# MAGIC   postal_code STRING,
# MAGIC   rowguid CHAR(36),
# MAGIC   modified_date TIMESTAMP,
# MAGIC   
# MAGIC   -- Change detection
# MAGIC   _tf_row_hash STRING,
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Audit columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL,
# MAGIC   _tf_change_type STRING
# MAGIC   
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'description' = 'Address dimension with SCD Type 2 historization'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.customer (
# MAGIC   _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC   
# MAGIC   -- Business key
# MAGIC   customer_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Attributes
# MAGIC   name_style BOOLEAN,
# MAGIC   title STRING,
# MAGIC   first_name STRING,
# MAGIC   middle_name STRING,
# MAGIC   last_name STRING,
# MAGIC   suffix STRING,
# MAGIC   company_name STRING,
# MAGIC   sales_person STRING,
# MAGIC   email_address STRING,
# MAGIC   phone STRING,
# MAGIC   password_hash STRING,
# MAGIC   password_salt STRING,
# MAGIC   rowguid CHAR(36),
# MAGIC   modified_date TIMESTAMP,
# MAGIC   
# MAGIC   -- Change detection
# MAGIC   _tf_row_hash STRING,
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Audit columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL,
# MAGIC   _tf_change_type STRING
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'description' = 'Customer dimension with SCD Type 2 historization'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.sales_order_header (
# MAGIC   _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL, -- Incremental surrogate key
# MAGIC   
# MAGIC   -- Business key
# MAGIC   sales_order_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Attributes
# MAGIC   revision_number SMALLINT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   due_date TIMESTAMP,
# MAGIC   ship_date TIMESTAMP,
# MAGIC   status SMALLINT,
# MAGIC   online_order_flag BOOLEAN,
# MAGIC   sales_order_number STRING,
# MAGIC   purchase_order_number STRING,
# MAGIC   account_number STRING,
# MAGIC   customer_id INT,
# MAGIC   ship_to_address_id INT,
# MAGIC   bill_to_address_id INT,
# MAGIC   ship_method STRING,
# MAGIC   credit_card_approval_code STRING,
# MAGIC   sub_total DECIMAL(19,4),
# MAGIC   tax_amt DECIMAL(19,4),
# MAGIC   freight DECIMAL(19,4),
# MAGIC   total_due DECIMAL(19,4),
# MAGIC   comment STRING,
# MAGIC   rowguid CHAR(36),
# MAGIC   modified_date TIMESTAMP,
# MAGIC   
# MAGIC   -- Change detection
# MAGIC   _tf_row_hash STRING,
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Audit columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL,
# MAGIC   _tf_change_type STRING
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'description' = 'Sales order header with SCD Type 2 historization'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.sales_order_detail (
# MAGIC   _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC   
# MAGIC   -- Composite business key
# MAGIC   sales_order_id INT NOT NULL,
# MAGIC   sales_order_detail_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Attributes
# MAGIC   order_qty SMALLINT,
# MAGIC   product_id INT,
# MAGIC   unit_price DECIMAL(19,4),
# MAGIC   unit_price_discount DECIMAL(19,4),
# MAGIC   line_total DECIMAL(38,6),
# MAGIC   rowguid CHAR(36),
# MAGIC   modified_date TIMESTAMP,
# MAGIC   
# MAGIC   -- Change detection
# MAGIC   _tf_row_hash STRING,
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Audit columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL,
# MAGIC   _tf_change_type STRING
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'description' = 'Sales order detail with SCD Type 2 historization'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.customeraddress (
# MAGIC   _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC   
# MAGIC   -- Composite business key
# MAGIC   customer_id INT NOT NULL,
# MAGIC   address_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Attributes
# MAGIC   address_type STRING,
# MAGIC   rowguid CHAR(36),
# MAGIC   modified_date TIMESTAMP,
# MAGIC   
# MAGIC   -- Change detection
# MAGIC   _tf_row_hash STRING,
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Audit columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL,
# MAGIC   _tf_change_type STRING
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'description' = 'Customer address bridge table with SCD Type 2'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables Silver supplémentaires - Produits et relations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.product (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Business key
# MAGIC    product_id INT NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    name STRING,
# MAGIC    product_number STRING,
# MAGIC    color STRING,
# MAGIC    standard_cost DECIMAL(19,4),
# MAGIC    list_price DECIMAL(19,4),
# MAGIC    size STRING,
# MAGIC    weight DECIMAL(8,2),
# MAGIC    product_category_id INT,
# MAGIC    product_model_id INT,
# MAGIC    sell_start_date TIMESTAMP,
# MAGIC    sell_end_date TIMESTAMP,
# MAGIC    discontinued_date TIMESTAMP,
# MAGIC    thumbnail_photo BINARY,
# MAGIC    thumbnail_photo_file_name STRING,
# MAGIC    rowguid CHAR(36),
# MAGIC    modified_date TIMESTAMP,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'Product dimension with SCD Type 2 historization'
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.productcategory (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Business key
# MAGIC    product_category_id INT NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    parent_product_category_id INT,
# MAGIC    name STRING,
# MAGIC    rowguid CHAR(36),
# MAGIC    modified_date TIMESTAMP,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'Product category hierarchy with SCD Type 2'
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.productdescription (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Business key
# MAGIC    product_description_id INT NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    description STRING,
# MAGIC    rowguid CHAR(36),
# MAGIC    modified_date TIMESTAMP,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'Product description with SCD Type 2'
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.productmodel (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Business key
# MAGIC    product_model_id INT NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    name STRING,
# MAGIC    catalog_description STRING,
# MAGIC    rowguid CHAR(36),
# MAGIC    modified_date TIMESTAMP,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'Product model with SCD Type 2'
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.productmodelproductdescription (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Composite business key
# MAGIC    product_model_id INT NOT NULL,
# MAGIC    product_description_id INT NOT NULL,
# MAGIC    culture STRING NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    rowguid CHAR(36),
# MAGIC    modified_date TIMESTAMP,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'Product model to description bridge table with SCD Type 2'
# MAGIC  );
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.vgetallcategories (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Business key
# MAGIC    product_category_id INT NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    parent_product_category_id INT,
# MAGIC    name STRING,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'View of all categories with SCD Type 2'
# MAGIC  );
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.vproductanddescription (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Composite business key
# MAGIC    product_id INT NOT NULL,
# MAGIC    culture STRING NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    name STRING,
# MAGIC    product_model STRING,
# MAGIC    description STRING,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'View of products and descriptions with SCD Type 2'
# MAGIC  );
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.silver.vproductmodelcatalogdescription (
# MAGIC    _tf_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC    
# MAGIC    -- Business key
# MAGIC    product_model_id INT NOT NULL,
# MAGIC    
# MAGIC    -- Attributes
# MAGIC    name STRING,
# MAGIC    summary STRING,
# MAGIC    manufacturer STRING,
# MAGIC    copyright STRING,
# MAGIC    producturl STRING,
# MAGIC    warrantyperiod STRING,
# MAGIC    warrantydescription STRING,
# MAGIC    noofyears STRING,
# MAGIC    maintenancedescription STRING,
# MAGIC    wheel STRING,
# MAGIC    saddle STRING,
# MAGIC    pedal STRING,
# MAGIC    bikeframe STRING,
# MAGIC    crankset STRING,
# MAGIC    pictureangle STRING,
# MAGIC    picturesize STRING,
# MAGIC    productphotoid STRING,
# MAGIC    material STRING,
# MAGIC    color STRING,
# MAGIC    productline STRING,
# MAGIC    style STRING,
# MAGIC    riderexperience STRING,
# MAGIC    rowguid CHAR(36),
# MAGIC    modifieddate TIMESTAMP,
# MAGIC    
# MAGIC    -- Change detection
# MAGIC    _tf_row_hash STRING,
# MAGIC    
# MAGIC    -- SCD Type 2 columns
# MAGIC    _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC    _tf_valid_to TIMESTAMP,
# MAGIC    _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC    
# MAGIC    -- Audit columns
# MAGIC    _tf_create_date TIMESTAMP NOT NULL,
# MAGIC    _tf_update_date TIMESTAMP NOT NULL,
# MAGIC    _tf_change_type STRING
# MAGIC  )
# MAGIC  TBLPROPERTIES (
# MAGIC    'delta.enableChangeDataFeed' = 'true',
# MAGIC    'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC    'description' = 'View of product model catalog descriptions with SCD Type 2'
# MAGIC  );
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables in Gold

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare mask function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création d'une fonction de masquage conditionnelle pour n'afficher la valeur que pour les membres du groupe 'admins'
# MAGIC CREATE OR REPLACE FUNCTION jeromeaymon_lakehouse.gold.mask_if_not_admin(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_member('admins') THEN val
# MAGIC     ELSE '***MASKED***'
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a managed table in our Unity Catalog
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.gold.dim_calendar AS
# MAGIC -- CTE to simplify our SQL
# MAGIC WITH calendar_dates AS (
# MAGIC     SELECT
# MAGIC         explode(array_dates) AS calendar_date
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             SEQUENCE(
# MAGIC                 MAKE_DATE(2000, 01, 01), -- Start date
# MAGIC                 MAKE_DATE(2030, 12, 31), -- End date
# MAGIC                 INTERVAL 1 DAY           -- Incremental step
# MAGIC             ) AS array_dates
# MAGIC     )
# MAGIC )
# MAGIC -- The SQL transforming our main calendar_date into all the columns we will be requiring
# MAGIC SELECT
# MAGIC     -- Calendar table primary key
# MAGIC     10000 * YEAR(calendar_date) + 100 * MONTH(calendar_date) + DAY(calendar_date) AS _tf_dim_calendar_sk,
# MAGIC     
# MAGIC     -- Date attributes
# MAGIC     TO_DATE(calendar_date) AS cal_date,
# MAGIC     YEAR(calendar_date) AS cal_year,
# MAGIC     MONTH(calendar_date) AS cal_month,
# MAGIC     DAY(calendar_date) AS cal_day_of_month,
# MAGIC     DATE_FORMAT(calendar_date, 'EEEE MMMM dd yyyy') AS cal_date_full,
# MAGIC     DATE_FORMAT(calendar_date, 'EEEE') AS cal_day_name,
# MAGIC     
# MAGIC     -- Week attributes
# MAGIC     CASE
# MAGIC         WHEN DATE_ADD(calendar_date, (WEEKDAY(calendar_date) + 1) - 1) = calendar_date THEN TO_DATE(calendar_date)
# MAGIC         ELSE DATE_ADD(calendar_date, -(WEEKDAY(calendar_date)))
# MAGIC     END AS cal_week_start,
# MAGIC     DATE_ADD(
# MAGIC         CASE
# MAGIC             WHEN DATE_ADD(calendar_date, (WEEKDAY(calendar_date) + 1) - 1) = calendar_date THEN TO_DATE(calendar_date)
# MAGIC             ELSE DATE_ADD(calendar_date, -(WEEKDAY(calendar_date)))
# MAGIC         END,
# MAGIC         6
# MAGIC     ) AS cal_week_end,
# MAGIC     WEEKDAY(calendar_date) + 1 AS cal_week_day,
# MAGIC     WEEKOFYEAR(calendar_date) AS cal_week_of_year,
# MAGIC     
# MAGIC     -- Month attributes
# MAGIC     DATE_FORMAT(calendar_date, 'MMMM yyyy') AS cal_month_year,
# MAGIC     DATE_FORMAT(calendar_date, 'MMMM') AS cal_month_name,
# MAGIC     DATE_ADD(LAST_DAY(ADD_MONTHS(calendar_date, -1)), 1) AS cal_first_day_of_month,
# MAGIC     LAST_DAY(calendar_date) AS cal_last_day_of_month,
# MAGIC     -- Quarter attributes
# MAGIC     CASE
# MAGIC         WHEN MONTH(calendar_date) IN (1, 2, 3) THEN 1
# MAGIC         WHEN MONTH(calendar_date) IN (4, 5, 6) THEN 2
# MAGIC         WHEN MONTH(calendar_date) IN (7, 8, 9) THEN 3
# MAGIC         ELSE 4
# MAGIC     END AS cal_quarter,
# MAGIC     CASE
# MAGIC         WHEN MONTH(calendar_date) IN (1, 2, 3) THEN 1
# MAGIC         WHEN MONTH(calendar_date) IN (4, 5, 6) THEN 2
# MAGIC         WHEN MONTH(calendar_date) IN (7, 8, 9) THEN 3
# MAGIC         ELSE 4
# MAGIC     END AS cal_fiscal_quarter,
# MAGIC     YEAR(DATE_ADD(calendar_date, 89)) AS cal_fiscal_year,
# MAGIC     
# MAGIC     -- Flags
# MAGIC     CASE WHEN WEEKDAY(calendar_date) IN (5, 6) THEN TRUE ELSE FALSE END AS cal_is_weekend,
# MAGIC     CASE WHEN DAY(calendar_date) = 1 THEN TRUE ELSE FALSE END AS cal_is_month_start,
# MAGIC     CASE WHEN calendar_date = LAST_DAY(calendar_date) THEN TRUE ELSE FALSE END AS cal_is_month_end,
# MAGIC     
# MAGIC     -- Technical columns
# MAGIC     current_timestamp() AS _tf_create_date,
# MAGIC     current_timestamp() AS _tf_update_date
# MAGIC FROM calendar_dates;
# MAGIC
# MAGIC ALTER TABLE jeromeaymon_lakehouse.gold.dim_calendar
# MAGIC ALTER COLUMN _tf_dim_calendar_sk SET NOT NULL;
# MAGIC ALTER TABLE jeromeaymon_lakehouse.gold.dim_calendar ADD PRIMARY KEY (_tf_dim_calendar_sk);
# MAGIC
# MAGIC COMMENT ON TABLE jeromeaymon_lakehouse.gold.dim_calendar IS 'Date dimension with calendar and fiscal attributes';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.gold.dim_geography (
# MAGIC     -- Surrogate key
# MAGIC     _tf_dim_geography_sk BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC     
# MAGIC     -- Business key
# MAGIC     geo_address_id INT NOT NULL,
# MAGIC     
# MAGIC     -- Attributes
# MAGIC     geo_address_line_1 STRING,
# MAGIC     geo_address_line_2 STRING,
# MAGIC     geo_city STRING,
# MAGIC     geo_state_province STRING,
# MAGIC     geo_country_region STRING,
# MAGIC     geo_postal_code STRING,
# MAGIC     
# MAGIC     -- SCD Type 2 columns
# MAGIC     _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC     _tf_valid_to TIMESTAMP,
# MAGIC     _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC     
# MAGIC     -- Technical columns
# MAGIC     _tf_create_date TIMESTAMP NOT NULL,
# MAGIC     _tf_update_date TIMESTAMP NOT NULL
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'description' = 'Geography dimension with SCD Type 2'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO jeromeaymon_lakehouse.gold.dim_geography (
# MAGIC     _tf_dim_geography_sk, 
# MAGIC     geo_address_id, 
# MAGIC     geo_address_line_1, 
# MAGIC     geo_address_line_2, 
# MAGIC     geo_city, 
# MAGIC     geo_state_province, 
# MAGIC     geo_country_region, 
# MAGIC     geo_postal_code, 
# MAGIC     _tf_valid_from, 
# MAGIC     _tf_valid_to, 
# MAGIC     _tf_create_date, 
# MAGIC     _tf_update_date)
# MAGIC VALUES (-9, 0, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', TIMESTAMP'2000-01-01 00:00:00', NULL, current_timestamp(), current_timestamp());
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.gold.dim_customer (
# MAGIC   -- Surrogate key
# MAGIC   _tf_dim_customer_sk BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC   
# MAGIC   -- Business key
# MAGIC   cust_customer_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Attributes
# MAGIC   cust_title STRING,
# MAGIC   cust_first_name STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   cust_middle_name STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   cust_last_name STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   cust_suffix STRING,
# MAGIC   cust_company_name STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   cust_sales_person STRING,
# MAGIC   cust_email_address STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   cust_phone STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   
# MAGIC   -- Derived attributes
# MAGIC   cust_full_name STRING MASK jeromeaymon_lakehouse.gold.mask_if_not_admin,
# MAGIC   cust_type STRING GENERATED ALWAYS AS (CASE WHEN cust_company_name IS NOT NULL THEN 'Business' ELSE 'Individual' END),
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'description' = 'Customer dimension with SCD Type 2'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert Unknown record
# MAGIC INSERT INTO jeromeaymon_lakehouse.gold.dim_customer (
# MAGIC   _tf_dim_customer_sk, 
# MAGIC   cust_customer_id,
# MAGIC   cust_title,
# MAGIC   cust_first_name,
# MAGIC   cust_middle_name,
# MAGIC   cust_last_name,
# MAGIC   cust_suffix,
# MAGIC   cust_company_name,
# MAGIC   cust_sales_person,
# MAGIC   cust_email_address,
# MAGIC   cust_phone,
# MAGIC   cust_full_name,
# MAGIC   _tf_valid_from,
# MAGIC   _tf_valid_to,
# MAGIC   _tf_create_date,
# MAGIC   _tf_update_date)
# MAGIC VALUES (
# MAGIC   -9, 0, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 
# MAGIC   'N/A', 'N/A', 'N/A', 'N/A', TIMESTAMP'2000-01-01 00:00:00', 
# MAGIC   NULL, current_timestamp(), current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension ProductCategory avec hiérarchie complète

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_product_category (
# MAGIC   -- Surrogate key
# MAGIC   _tf_dim_product_category_sk BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC   
# MAGIC   -- Business key
# MAGIC   prod_cat_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Niveau actuel (leaf level)
# MAGIC   prod_cat_name STRING,
# MAGIC   
# MAGIC   -- Niveau parent (Level 1)
# MAGIC   prod_cat_parent_id INT,
# MAGIC   prod_cat_parent_name STRING,
# MAGIC   
# MAGIC   -- Niveau grand-parent (Level 2) si applicable
# MAGIC   prod_cat_grandparent_id INT,
# MAGIC   prod_cat_grandparent_name STRING,
# MAGIC   
# MAGIC   -- Attributs hiérarchiques
# MAGIC   prod_cat_level INT, -- Profondeur dans la hiérarchie (1=root, 2=child, 3=grandchild)
# MAGIC   prod_cat_hierarchy_path STRING, -- Chemin complet (ex: "Bikes > Mountain Bikes > Mountain-100")
# MAGIC   prod_cat_is_leaf BOOLEAN, -- TRUE si c'est une catégorie terminale
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'description' = 'Product category dimension with full hierarchy - SCD Type 2'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert Unknown record
# MAGIC INSERT INTO gold.dim_product_category (
# MAGIC   _tf_dim_product_category_sk, prod_cat_id, prod_cat_name,
# MAGIC   prod_cat_parent_id, prod_cat_parent_name,
# MAGIC   prod_cat_grandparent_id, prod_cat_grandparent_name,
# MAGIC   prod_cat_level, prod_cat_hierarchy_path, prod_cat_is_leaf,
# MAGIC   _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date
# MAGIC )
# MAGIC VALUES (
# MAGIC   -9, 0, 'Unknown', 
# MAGIC   NULL, NULL,
# MAGIC   NULL, NULL,
# MAGIC   0, 'Unknown', TRUE,
# MAGIC   TIMESTAMP'2000-01-01 00:00:00', NULL, current_timestamp(), current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Product avec références au Model et à la Category
# MAGIC ### Création de la table dim_product

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_product (
# MAGIC   -- Surrogate key
# MAGIC   _tf_dim_product_sk BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC   
# MAGIC   -- Business key
# MAGIC   prod_product_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Product attributes
# MAGIC   prod_name STRING,
# MAGIC   prod_product_number STRING,
# MAGIC   prod_color STRING,
# MAGIC   prod_standard_cost DECIMAL(19,4),
# MAGIC   prod_list_price DECIMAL(19,4),
# MAGIC   prod_size STRING,
# MAGIC   prod_weight DECIMAL(8,2),
# MAGIC   prod_sell_start_date DATE,
# MAGIC   prod_sell_end_date DATE,
# MAGIC   prod_discontinued_date DATE,
# MAGIC   
# MAGIC   -- Foreign key to Category dimension
# MAGIC   _tf_dim_product_category_sk BIGINT,
# MAGIC   
# MAGIC   -- Category attributes (denormalized for performance)
# MAGIC   prod_category_id INT,
# MAGIC   prod_category_name STRING,
# MAGIC   prod_category_hierarchy STRING,
# MAGIC   
# MAGIC   -- Model attributes (denormalized)
# MAGIC   prod_model_id INT,
# MAGIC   prod_model_name STRING,
# MAGIC   
# MAGIC   -- Derived attributes
# MAGIC   prod_price_range STRING,
# MAGIC   prod_weight_range STRING,
# MAGIC   prod_is_active BOOLEAN,
# MAGIC   prod_days_to_sell INT,
# MAGIC   prod_profit_margin DECIMAL(10,2),
# MAGIC   
# MAGIC   -- SCD Type 2 columns
# MAGIC   _tf_valid_from TIMESTAMP NOT NULL,
# MAGIC   _tf_valid_to TIMESTAMP,
# MAGIC   _tf_is_current BOOLEAN GENERATED ALWAYS AS (_tf_valid_to IS NULL),
# MAGIC   
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP NOT NULL,
# MAGIC   
# MAGIC   -- Foreign key constraint (optional, depends on Databricks version)
# MAGIC   CONSTRAINT fk_product_category FOREIGN KEY (_tf_dim_product_category_sk) 
# MAGIC     REFERENCES gold.dim_product_category(_tf_dim_product_category_sk)
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'description' = 'Product dimension with category and model denormalized - SCD Type 2'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert Unknown record
# MAGIC INSERT INTO gold.dim_product (
# MAGIC   _tf_dim_product_sk, prod_product_id, prod_name, prod_product_number,
# MAGIC   prod_color, prod_standard_cost, prod_list_price, prod_size, prod_weight,
# MAGIC   prod_sell_start_date, prod_sell_end_date, prod_discontinued_date,
# MAGIC   _tf_dim_product_category_sk, prod_category_id, prod_category_name, prod_category_hierarchy,
# MAGIC   prod_model_id, prod_model_name,
# MAGIC   prod_price_range, prod_weight_range, prod_is_active, prod_days_to_sell, prod_profit_margin,
# MAGIC   _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date
# MAGIC )
# MAGIC VALUES (
# MAGIC   -9, 0, 'Unknown', 'Unknown', 'Unknown', 0.0, 0.0, 'Unknown', 0.0,
# MAGIC   DATE'2000-01-01', NULL, NULL,
# MAGIC   -9, 0, 'Unknown', 'Unknown',
# MAGIC   0, 'Unknown',
# MAGIC   'Unknown', 'Unknown', FALSE, 0, 0.0,
# MAGIC   TIMESTAMP'2000-01-01 00:00:00', NULL, current_timestamp(), current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Création table de Fact

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE jeromeaymon_lakehouse.gold.fact_sales (
# MAGIC   -- Surrogate key
# MAGIC   _tf_fact_sales_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
# MAGIC   
# MAGIC   -- Business keys
# MAGIC   sales_order_id INT NOT NULL,
# MAGIC   sales_order_detail_id INT NOT NULL,
# MAGIC   
# MAGIC   -- Foreign keys to dimensions
# MAGIC   _tf_dim_calendar_sk INT 
# MAGIC   REFERENCES jeromeaymon_lakehouse.gold.dim_calendar(_tf_dim_calendar_sk),
# MAGIC   _tf_dim_customer_sk BIGINT 
# MAGIC   REFERENCES jeromeaymon_lakehouse.gold.dim_customer(_tf_dim_customer_sk),
# MAGIC   _tf_dim_geography_sk BIGINT NOT NULL 
# MAGIC   REFERENCES jeromeaymon_lakehouse.gold.dim_geography(_tf_dim_geography_sk),
# MAGIC   _tf_dim_product_sk BIGINT 
# MAGIC   REFERENCES jeromeaymon_lakehouse.gold.dim_product(_tf_dim_product_sk),
# MAGIC     
# MAGIC   -- Measures
# MAGIC   sales_order_qty SMALLINT,
# MAGIC   sales_unit_price DECIMAL(19, 4),
# MAGIC   sales_unit_price_discount DECIMAL(19, 4),
# MAGIC   sales_line_total DECIMAL(38, 6),
# MAGIC   
# MAGIC   -- Calculated measures
# MAGIC   sales_discount_amount DECIMAL(38,6) GENERATED ALWAYS AS (
# MAGIC     sales_order_qty * sales_unit_price * sales_unit_price_discount
# MAGIC   ),
# MAGIC   sales_net_amount DECIMAL(38,6) GENERATED ALWAYS AS (
# MAGIC     sales_line_total
# MAGIC   ),
# MAGIC   
# MAGIC   -- Technical columns
# MAGIC   _tf_create_date TIMESTAMP NOT NULL,
# MAGIC   _tf_update_date TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'description' = 'Sales fact table at order detail grain'
# MAGIC );
