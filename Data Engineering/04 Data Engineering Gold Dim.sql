-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Loading the Dim tables in the Gold layer 
-- MAGIC ## Connecting to the Gold layer (Target)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Améliorations:**
-- MAGIC  - SCD Type 2 pour les dimensions avec historisation complète
-- MAGIC  - Surrogate keys pour l'indépendance
-- MAGIC  - Attributs dérivés et enrichis
-- MAGIC  - Gestion des N/A records
-- MAGIC  - Normalisation des données

-- COMMAND ----------

USE CATALOG jeromeaymon_lakehouse;
USE SCHEMA gold;

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension Geography avec SCD Type 2
-- MAGIC  

-- COMMAND ----------

-- DBTITLE 1,Cell 5
CREATE OR REPLACE TEMP VIEW _src_dim_geography AS
SELECT
  CAST(address_id AS INT) AS geo_address_id,
  UPPER(TRIM(COALESCE(address_line1, 'N/A'))) AS geo_address_line_1,
  UPPER(TRIM(COALESCE(address_line2, ''))) AS geo_address_line_2,
  UPPER(TRIM(COALESCE(city, 'N/A'))) AS geo_city,
  UPPER(TRIM(COALESCE(state_province, 'N/A'))) AS geo_state_province,
  UPPER(TRIM(COALESCE(country_region, 'N/A'))) AS geo_country_region,
  REPLACE(COALESCE(postal_code, '00000'), ' ', '') AS geo_postal_code
FROM silver.address
WHERE _tf_is_current = TRUE;

-- COMMAND ----------

-- MERGE: Update existing records or insert new ones
MERGE INTO gold.dim_geography AS tgt
USING _src_dim_geography AS src
ON tgt.geo_address_id = src.geo_address_id
  AND tgt._tf_is_current = TRUE

-- Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.geo_address_line_1 != src.geo_address_line_1 OR
    tgt.geo_address_line_2 != src.geo_address_line_2 OR
    tgt.geo_city != src.geo_city OR
    tgt.geo_state_province != src.geo_state_province OR
    tgt.geo_country_region != src.geo_country_region OR
    tgt.geo_postal_code != src.geo_postal_code
) THEN 
  UPDATE SET 
    tgt.geo_address_line_1 = src.geo_address_line_1,
    tgt.geo_address_line_2 = src.geo_address_line_2,
    tgt.geo_city = src.geo_city,
    tgt.geo_state_province = src.geo_state_province,
    tgt.geo_country_region = src.geo_country_region,
    tgt.geo_postal_code = src.geo_postal_code,
    tgt._tf_update_date = load_date

-- Insert new records
WHEN NOT MATCHED THEN
  INSERT (
    geo_address_id, 
    geo_address_line_1, 
    geo_address_line_2, 
    geo_city,
    geo_state_province, 
    geo_country_region, 
    geo_postal_code, 
    _tf_valid_from, 
    _tf_valid_to, 
    _tf_create_date, 
    _tf_update_date
  )
  VALUES (
    src.geo_address_id,
    src.geo_address_line_1,
    src.geo_address_line_2,
    src.geo_city,
    src.geo_state_province,
    src.geo_country_region,
    src.geo_postal_code,
    load_date,
    NULL,
    load_date,
    load_date
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension Customer avec SCD Type 2
-- MAGIC  

-- COMMAND ----------

-- DBTITLE 1,Cell 7
CREATE OR REPLACE TEMP VIEW _src_dim_customer AS
SELECT
  CAST(customer_id AS INT) AS cust_customer_id,
  TRIM(COALESCE(title, '')) AS cust_title,
  TRIM(COALESCE(first_name, 'N/A')) AS cust_first_name,
  TRIM(COALESCE(middle_name, '')) AS cust_middle_name,
  TRIM(COALESCE(last_name, 'N/A')) AS cust_last_name,
  TRIM(COALESCE(suffix, '')) AS cust_suffix,
  TRIM(COALESCE(company_name, '')) AS cust_company_name,
  TRIM(COALESCE(sales_person, '')) AS cust_sales_person,
  LOWER(TRIM(COALESCE(email_address, 'N/A@N/A.com'))) AS cust_email_address,
  TRIM(COALESCE(phone, '000-000-0000')) AS cust_phone,
  -- Attribut dérivé: nom complet
  CONCAT_WS(' ',
    NULLIF(TRIM(COALESCE(title, '')), ''),
    TRIM(COALESCE(first_name, '')),
    NULLIF(TRIM(COALESCE(middle_name, '')), ''),
    TRIM(COALESCE(last_name, '')),
    NULLIF(TRIM(COALESCE(suffix, '')), '')
  ) AS cust_full_name
FROM silver.customer
WHERE _tf_is_current = TRUE;

-- COMMAND ----------

MERGE INTO gold.dim_customer AS tgt
USING _src_dim_customer AS src
ON tgt.cust_customer_id = src.cust_customer_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND (
  tgt.cust_title != src.cust_title OR
  tgt.cust_first_name != src.cust_first_name OR
  tgt.cust_middle_name != src.cust_middle_name OR
  tgt.cust_last_name != src.cust_last_name OR
  tgt.cust_suffix != src.cust_suffix OR
  tgt.cust_company_name != src.cust_company_name OR
  tgt.cust_sales_person != src.cust_sales_person OR
  tgt.cust_email_address != src.cust_email_address OR
  tgt.cust_phone != src.cust_phone
) THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date

-- Insert new records
WHEN NOT MATCHED THEN
  INSERT (
    cust_customer_id, cust_title, cust_first_name, cust_middle_name, cust_last_name,
    cust_suffix, cust_company_name, cust_sales_person, cust_email_address,
    cust_phone, cust_full_name, _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date
  )
  VALUES (
    src.cust_customer_id,
    src.cust_title,
    src.cust_first_name,
    src.cust_middle_name,
    src.cust_last_name,
    src.cust_suffix,
    src.cust_company_name,
    src.cust_sales_person,
    src.cust_email_address,
    src.cust_phone,
    src.cust_full_name,
    load_date,
    NULL,
    load_date,
    load_date
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension ProductCategory avec hiérarchie complète
-- MAGIC ### Chargement de dim_product_category avec hiérarchie complète

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_dim_product_category AS
WITH category_hierarchy AS (
  -- Catégories avec leur parent direct
  SELECT
    pc.product_category_id,
    pc.name AS category_name,
    pc.parent_product_category_id,
    parent_pc.name AS parent_name,
    parent_pc.parent_product_category_id AS grandparent_id
  FROM silver.productcategory pc
  LEFT JOIN silver.productcategory parent_pc
    ON pc.parent_product_category_id = parent_pc.product_category_id
    AND parent_pc._tf_is_current = TRUE
  WHERE pc._tf_is_current = TRUE
),
category_with_grandparent AS (
  -- Ajout du grand-parent
  SELECT
    ch.*,
    grandparent_pc.name AS grandparent_name
  FROM category_hierarchy ch
  LEFT JOIN silver.productcategory grandparent_pc
    ON ch.grandparent_id = grandparent_pc.product_category_id
    AND grandparent_pc._tf_is_current = TRUE
),
category_with_level AS (
  -- Calcul du niveau et du chemin hiérarchique
  SELECT
    product_category_id,
    category_name,
    parent_product_category_id,
    parent_name,
    grandparent_id,
    grandparent_name,
    -- Calcul du niveau
    CASE
      WHEN parent_product_category_id IS NULL THEN 1 -- Root
      WHEN grandparent_id IS NULL THEN 2 -- Child of root
      ELSE 3 -- Grandchild
    END AS category_level,
    -- Construction du chemin hiérarchique
    CASE
      WHEN parent_product_category_id IS NULL THEN category_name
      WHEN grandparent_id IS NULL THEN CONCAT(parent_name, ' > ', category_name)
      ELSE CONCAT(grandparent_name, ' > ', parent_name, ' > ', category_name)
    END AS hierarchy_path,
    -- Déterminer si c'est une feuille (n'a pas d'enfants)
    NOT EXISTS (
      SELECT 1 
      FROM silver.productcategory child
      WHERE child.parent_product_category_id = product_category_id
        AND child._tf_is_current = TRUE
    ) AS is_leaf
  FROM category_with_grandparent
)
SELECT
  CAST(product_category_id AS INT) AS prod_cat_id,
  TRIM(COALESCE(category_name, 'N/A')) AS prod_cat_name,
  CAST(parent_product_category_id AS INT) AS prod_cat_parent_id,
  TRIM(COALESCE(parent_name, '')) AS prod_cat_parent_name,
  CAST(grandparent_id AS INT) AS prod_cat_grandparent_id,
  TRIM(COALESCE(grandparent_name, '')) AS prod_cat_grandparent_name,
  category_level AS prod_cat_level,
  hierarchy_path AS prod_cat_hierarchy_path,
  is_leaf AS prod_cat_is_leaf
FROM category_with_level;

-- COMMAND ----------

-- Fermer les enregistrements modifiés
MERGE INTO gold.dim_product_category AS tgt
USING _src_dim_product_category AS src
ON tgt.prod_cat_id = src.prod_cat_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND (
  tgt.prod_cat_name != src.prod_cat_name OR
  COALESCE(tgt.prod_cat_parent_id, -1) != COALESCE(src.prod_cat_parent_id, -1) OR
  COALESCE(tgt.prod_cat_parent_name, '') != COALESCE(src.prod_cat_parent_name, '') OR
  tgt.prod_cat_level != src.prod_cat_level OR
  tgt.prod_cat_hierarchy_path != src.prod_cat_hierarchy_path OR
  tgt.prod_cat_is_leaf != src.prod_cat_is_leaf
) THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date;

-- COMMAND ----------

-- Insérer nouvelles versions
INSERT INTO gold.dim_product_category (
  prod_cat_id, prod_cat_name, prod_cat_parent_id, prod_cat_parent_name,
  prod_cat_grandparent_id, prod_cat_grandparent_name,
  prod_cat_level, prod_cat_hierarchy_path, prod_cat_is_leaf,
  _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date
)
SELECT
  src.prod_cat_id,
  src.prod_cat_name,
  src.prod_cat_parent_id,
  src.prod_cat_parent_name,
  src.prod_cat_grandparent_id,
  src.prod_cat_grandparent_name,
  src.prod_cat_level,
  src.prod_cat_hierarchy_path,
  src.prod_cat_is_leaf,
  load_date,
  NULL,
  load_date,
  load_date
FROM _src_dim_product_category src
WHERE NOT EXISTS (
  SELECT 1 FROM gold.dim_product_category tgt
  WHERE tgt.prod_cat_id = src.prod_cat_id
    AND tgt._tf_is_current = TRUE
    AND tgt.prod_cat_name = src.prod_cat_name
    AND COALESCE(tgt.prod_cat_parent_id, -1) = COALESCE(src.prod_cat_parent_id, -1)
    AND tgt.prod_cat_hierarchy_path = src.prod_cat_hierarchy_path
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chargement de dim_product

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _src_dim_product AS
SELECT
  CAST(p.product_id AS INT) AS prod_product_id,
  TRIM(COALESCE(p.name, 'N/A')) AS prod_name,
  TRIM(COALESCE(p.product_number, 'N/A')) AS prod_product_number,
  TRIM(COALESCE(p.color, 'N/A')) AS prod_color,
  COALESCE(p.standard_cost, 0.0) AS prod_standard_cost,
  COALESCE(p.list_price, 0.0) AS prod_list_price,
  TRIM(COALESCE(p.size, 'N/A')) AS prod_size,
  COALESCE(p.weight, 0.0) AS prod_weight,
  CAST(p.sell_start_date AS DATE) AS prod_sell_start_date,
  CAST(p.sell_end_date AS DATE) AS prod_sell_end_date,
  CAST(p.discontinued_date AS DATE) AS prod_discontinued_date,
  
  -- Foreign key to category dimension
  COALESCE(dpc._tf_dim_product_category_sk, -9) AS _tf_dim_product_category_sk,
  
  -- Category attributes (denormalized)
  COALESCE(p.product_category_id, 0) AS prod_category_id,
  COALESCE(dpc.prod_cat_name, 'N/A') AS prod_category_name,
  COALESCE(dpc.prod_cat_hierarchy_path, 'N/A') AS prod_category_hierarchy,
  
  -- Model attributes
  COALESCE(p.product_model_id, 0) AS prod_model_id,
  COALESCE(pm.name, 'N/A') AS prod_model_name,
  
  -- Derived attributes: Price Range
  CASE
    WHEN p.list_price < 100 THEN 'Budget (< 100)'
    WHEN p.list_price < 500 THEN 'Mid-Range (100-500)'
    WHEN p.list_price < 1000 THEN 'Premium (500-1000)'
    WHEN p.list_price < 3000 THEN 'Luxury (1000-3000)'
    ELSE 'Ultra-Luxury (3000+)'
  END AS prod_price_range,
  
  -- Derived attributes: Weight Range
  CASE
    WHEN p.weight IS NULL THEN 'N/A'
    WHEN p.weight < 5 THEN 'Very Light (< 5 kg)'
    WHEN p.weight < 10 THEN 'Light (5-10 kg)'
    WHEN p.weight < 20 THEN 'Medium (10-20 kg)'
    ELSE 'Heavy (20+ kg)'
  END AS prod_weight_range,
  
  -- Derived attributes: Is Active
  CASE
    WHEN p.discontinued_date IS NOT NULL THEN FALSE
    WHEN p.sell_end_date IS NOT NULL AND p.sell_end_date < CURRENT_DATE() THEN FALSE
    ELSE TRUE
  END AS prod_is_active,
  
  -- Derived attributes: Days available to sell
  CASE
    WHEN p.sell_end_date IS NOT NULL THEN DATEDIFF(p.sell_end_date, p.sell_start_date)
    ELSE DATEDIFF(CURRENT_DATE(), p.sell_start_date)
  END AS prod_days_to_sell,
  
  -- Derived attributes: Profit Margin
  CASE
    WHEN p.standard_cost > 0 THEN 
      ROUND(((p.list_price - p.standard_cost) / p.standard_cost) * 100, 2)
    ELSE 0.0
  END AS prod_profit_margin

FROM silver.product p

-- Join avec product category dimension
LEFT JOIN gold.dim_product_category dpc
  ON p.product_category_id = dpc.prod_cat_id
  AND dpc._tf_is_current = TRUE

-- Join avec product model
LEFT JOIN silver.productmodel pm
  ON p.product_model_id = pm.product_model_id
  AND pm._tf_is_current = TRUE

WHERE p._tf_is_current = TRUE;

-- COMMAND ----------

-- Fermer les enregistrements modifiés
MERGE INTO gold.dim_product AS tgt
USING _src_dim_product AS src
ON tgt.prod_product_id = src.prod_product_id
  AND tgt._tf_is_current = TRUE
WHEN MATCHED AND (
  tgt.prod_name != src.prod_name OR
  tgt.prod_product_number != src.prod_product_number OR
  tgt.prod_color != src.prod_color OR
  tgt.prod_standard_cost != src.prod_standard_cost OR
  tgt.prod_list_price != src.prod_list_price OR
  tgt.prod_size != src.prod_size OR
  tgt.prod_weight != src.prod_weight OR
  COALESCE(tgt.prod_category_id, -1) != COALESCE(src.prod_category_id, -1) OR
  COALESCE(tgt.prod_model_id, -1) != COALESCE(src.prod_model_id, -1) OR
  tgt.prod_is_active != src.prod_is_active
) THEN
  UPDATE SET
    tgt._tf_valid_to = load_date,
    tgt._tf_update_date = load_date;

-- COMMAND ----------

-- Insérer nouvelles versions
INSERT INTO gold.dim_product (
  prod_product_id, prod_name, prod_product_number, prod_color,
  prod_standard_cost, prod_list_price, prod_size, prod_weight,
  prod_sell_start_date, prod_sell_end_date, prod_discontinued_date,
  _tf_dim_product_category_sk, prod_category_id, prod_category_name, prod_category_hierarchy,
  prod_model_id, prod_model_name,
  prod_price_range, prod_weight_range, prod_is_active, prod_days_to_sell, prod_profit_margin,
  _tf_valid_from, _tf_valid_to, _tf_create_date, _tf_update_date
)
SELECT
  src.prod_product_id,
  src.prod_name,
  src.prod_product_number,
  src.prod_color,
  src.prod_standard_cost,
  src.prod_list_price,
  src.prod_size,
  src.prod_weight,
  src.prod_sell_start_date,
  src.prod_sell_end_date,
  src.prod_discontinued_date,
  src._tf_dim_product_category_sk,
  src.prod_category_id,
  src.prod_category_name,
  src.prod_category_hierarchy,
  src.prod_model_id,
  src.prod_model_name,
  src.prod_price_range,
  src.prod_weight_range,
  src.prod_is_active,
  src.prod_days_to_sell,
  src.prod_profit_margin,
  load_date,
  NULL,
  load_date,
  load_date
FROM _src_dim_product src
WHERE NOT EXISTS (
  SELECT 1 FROM gold.dim_product tgt
  WHERE tgt.prod_product_id = src.prod_product_id
    AND tgt._tf_is_current = TRUE
    AND tgt.prod_name = src.prod_name
    AND tgt.prod_list_price = src.prod_list_price
    AND tgt.prod_is_active = src.prod_is_active
);
