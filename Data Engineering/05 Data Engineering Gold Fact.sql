-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Loading the Fact tables in the Gold layer 
-- MAGIC ## Connecting to the Gold layer (Target)

-- COMMAND ----------

USE CATALOG jeromeaymon_lakehouse;
USE SCHEMA gold;

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW _tmp_fact_sales AS
SELECT
  -- Business keys
  CAST(soh.sales_order_id AS INT) AS sales_order_id,
  CAST(sod.sales_order_detail_id AS INT) AS sales_order_detail_id,
  
  -- Dimension foreign keys
  10000 * YEAR(soh.order_date) + 100 * MONTH(soh.order_date) + DAY(soh.order_date) AS _tf_dim_calendar_sk,
  COALESCE(cust._tf_dim_customer_sk, -9) AS _tf_dim_customer_sk,
  COALESCE(geo._tf_dim_geography_sk, -9) AS _tf_dim_geography_sk,
  COALESCE(prod._tf_dim_product_sk, -9) AS _tf_dim_product_sk,
  
  -- Measures
  COALESCE(TRY_CAST(sod.order_qty AS SMALLINT), 0) AS sales_order_qty,
  COALESCE(TRY_CAST(sod.unit_price AS DECIMAL(19,4)), 0) AS sales_unit_price,
  COALESCE(TRY_CAST(sod.unit_price_discount AS DECIMAL(19,4)), 0) AS sales_unit_price_discount,
  COALESCE(TRY_CAST(sod.line_total AS DECIMAL(38, 6)), 0) AS sales_line_total

FROM silver.sales_order_detail sod

-- Join avec sales_order_header
LEFT OUTER JOIN silver.sales_order_header soh 
  ON sod.sales_order_id = soh.sales_order_id 
  AND soh._tf_valid_to IS NULL

-- Join avec customer
LEFT OUTER JOIN silver.customer c 
  ON soh.customer_id = c.customer_id 
  AND c._tf_valid_to IS NULL

-- Join avec dim_customer
LEFT OUTER JOIN gold.dim_customer cust
  ON c.customer_id = cust.cust_customer_id
  AND cust._tf_is_current = TRUE

-- Join avec address
LEFT OUTER JOIN silver.address a 
  ON soh.bill_to_address_id = a.address_id 
  AND a._tf_valid_to IS NULL

-- Join avec dim_geography
LEFT OUTER JOIN gold.dim_geography geo 
  ON a.address_id = geo.geo_address_id
  AND geo._tf_is_current = TRUE

-- Join avec product
LEFT OUTER JOIN silver.product p
  ON sod.product_id = p.product_id
  AND p._tf_valid_to IS NULL

-- Join avec dim_product
LEFT OUTER JOIN gold.dim_product prod
  ON p.product_id = prod.prod_product_id
  AND prod._tf_is_current = TRUE

WHERE sod._tf_valid_to IS NULL;

-- Vérifier la vue temporaire
SELECT * FROM _tmp_fact_sales LIMIT 10;

-- COMMAND ----------

SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT sales_order_id) AS distinct_orders,
  COUNT(DISTINCT sales_order_detail_id) AS distinct_details,
  SUM(CASE WHEN _tf_dim_customer_sk = -9 THEN 1 ELSE 0 END) AS unknown_customers,
  SUM(CASE WHEN _tf_dim_geography_sk = -9 THEN 1 ELSE 0 END) AS unknown_geographies,
  SUM(CASE WHEN _tf_dim_product_sk = -9 THEN 1 ELSE 0 END) AS unknown_products,
  SUM(sales_line_total) AS total_sales
FROM _tmp_fact_sales;

-- COMMAND ----------

-- DBTITLE 1,Cell 4
MERGE INTO gold.fact_sales AS tgt
USING _tmp_fact_sales AS src
ON tgt.sales_order_detail_id = src.sales_order_detail_id 
  AND tgt.sales_order_id = src.sales_order_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
  tgt._tf_dim_calendar_sk != src._tf_dim_calendar_sk OR
  tgt._tf_dim_customer_sk != src._tf_dim_customer_sk OR
  tgt._tf_dim_geography_sk != src._tf_dim_geography_sk OR
  tgt._tf_dim_product_sk != src._tf_dim_product_sk OR
  tgt.sales_order_qty != src.sales_order_qty OR
  tgt.sales_unit_price != src.sales_unit_price OR
  tgt.sales_unit_price_discount != src.sales_unit_price_discount OR
  tgt.sales_line_total != src.sales_line_total
) THEN 
  UPDATE SET 
    tgt._tf_dim_calendar_sk = src._tf_dim_calendar_sk,
    tgt._tf_dim_customer_sk = src._tf_dim_customer_sk,
    tgt._tf_dim_geography_sk = src._tf_dim_geography_sk,
    tgt._tf_dim_product_sk = src._tf_dim_product_sk,
    tgt.sales_order_qty = src.sales_order_qty,
    tgt.sales_unit_price = src.sales_unit_price,
    tgt.sales_unit_price_discount = src.sales_unit_price_discount,
    tgt.sales_line_total = src.sales_line_total,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  INSERT (
    sales_order_id,
    sales_order_detail_id,
    _tf_dim_calendar_sk,
    _tf_dim_customer_sk,
    _tf_dim_geography_sk,
    _tf_dim_product_sk,
    sales_order_qty,
    sales_unit_price,
    sales_unit_price_discount,
    sales_line_total,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.sales_order_id,
    src.sales_order_detail_id,
    src._tf_dim_calendar_sk,
    src._tf_dim_customer_sk,
    src._tf_dim_geography_sk,
    src._tf_dim_product_sk,
    src.sales_order_qty,
    src.sales_unit_price,
    src.sales_unit_price_discount,
    src.sales_line_total,
    load_date,  -- _tf_create_date
    load_date   -- _tf_update_date
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Afficher le résultat du MERGE

-- COMMAND ----------

-- DBTITLE 1,Cell 6
SELECT 
  'Merge completed' AS status,
  current_timestamp() AS execution_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contrôles de qualité de fact_sales

-- COMMAND ----------

-- Vérifier l'intégrité référentielle
SELECT 
  'Missing Calendar Keys' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM gold.dim_calendar c
  WHERE c._tf_dim_calendar_sk = f._tf_dim_calendar_sk
)
AND f._tf_dim_calendar_sk != -1

UNION ALL

SELECT 
  'Missing Customer Keys' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM gold.dim_customer c
  WHERE c._tf_dim_customer_sk = f._tf_dim_customer_sk
)
AND f._tf_dim_customer_sk != -9

UNION ALL

SELECT 
  'Missing Geography Keys' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM gold.dim_geography g
  WHERE g._tf_dim_geography_sk = f._tf_dim_geography_sk
)
AND f._tf_dim_geography_sk != -9

UNION ALL

SELECT 
  'Missing Product Keys' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM gold.dim_product p
  WHERE p._tf_dim_product_sk = f._tf_dim_product_sk
)
AND f._tf_dim_product_sk != -9;

-- COMMAND ----------

-- Vérifier les contraintes métier
SELECT 
  'Negative Quantities' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales
WHERE sales_order_qty <= 0

UNION ALL

SELECT 
  'Negative Amounts' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales
WHERE sales_line_total < 0

UNION ALL

SELECT 
  'Invalid Discounts' AS check_type,
  COUNT(*) AS violation_count
FROM gold.fact_sales
WHERE sales_unit_price_discount < 0 OR sales_unit_price_discount > 1;

-- COMMAND ----------

-- Statistiques de la table de faits
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT sales_order_id) AS unique_orders,
  MIN(_tf_dim_calendar_sk) AS earliest_date_key,
  MAX(_tf_dim_calendar_sk) AS latest_date_key,
  SUM(sales_order_qty) AS total_quantity,
  SUM(sales_line_total) AS total_revenue,
  AVG(sales_unit_price) AS avg_unit_price,
  SUM(CASE WHEN _tf_dim_customer_sk = -9 THEN 1 ELSE 0 END) AS unknown_customers_count,
  SUM(CASE WHEN _tf_dim_geography_sk = -9 THEN 1 ELSE 0 END) AS unknown_geography_count,
  SUM(CASE WHEN _tf_dim_product_sk = -9 THEN 1 ELSE 0 END) AS unknown_product_count
FROM gold.fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analyses business

-- COMMAND ----------

-- Top 10 clients par revenu
SELECT 
  c.cust_customer_id,
  c.cust_full_name,
  c.cust_company_name,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  SUM(f.sales_order_qty) AS total_quantity,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_line_total) AS avg_order_value
FROM gold.fact_sales f
INNER JOIN gold.dim_customer c
  ON f._tf_dim_customer_sk = c._tf_dim_customer_sk
  AND c._tf_is_current = TRUE
WHERE f._tf_dim_customer_sk != -9
GROUP BY c.cust_customer_id, c.cust_full_name, c.cust_company_name
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

-- Top 10 produits par revenu
SELECT 
  p.prod_product_id,
  p.prod_name,
  p.prod_category_hierarchy,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  SUM(f.sales_order_qty) AS total_quantity_sold,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_unit_price) AS avg_selling_price,
  p.prod_list_price,
  p.prod_profit_margin
FROM gold.fact_sales f
INNER JOIN gold.dim_product p
  ON f._tf_dim_product_sk = p._tf_dim_product_sk
  AND p._tf_is_current = TRUE
WHERE f._tf_dim_product_sk != -9
GROUP BY 
  p.prod_product_id, 
  p.prod_name, 
  p.prod_category_hierarchy,
  p.prod_list_price,
  p.prod_profit_margin
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

-- Ventes par catégorie de produits
SELECT 
  pc.prod_cat_level,
  pc.prod_cat_hierarchy_path,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  COUNT(DISTINCT p.prod_product_id) AS distinct_products_sold,
  SUM(f.sales_order_qty) AS total_quantity,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_unit_price) AS avg_unit_price
FROM gold.fact_sales f
INNER JOIN gold.dim_product p
  ON f._tf_dim_product_sk = p._tf_dim_product_sk
  AND p._tf_is_current = TRUE
INNER JOIN gold.dim_product_category pc
  ON p._tf_dim_product_category_sk = pc._tf_dim_product_category_sk
  AND pc._tf_is_current = TRUE
WHERE f._tf_dim_product_sk != -9
GROUP BY pc.prod_cat_level, pc.prod_cat_hierarchy_path
ORDER BY total_revenue DESC
LIMIT 20;

-- COMMAND ----------

-- Ventes par mois
SELECT 
  cal.cal_year,
  cal.cal_month,
  cal.cal_month_name,
  cal.cal_quarter,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  COUNT(*) AS line_count,
  SUM(f.sales_order_qty) AS total_quantity,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_line_total) AS avg_line_value
FROM gold.fact_sales f
INNER JOIN gold.dim_calendar cal
  ON f._tf_dim_calendar_sk = cal._tf_dim_calendar_sk
GROUP BY 
  cal.cal_year, 
  cal.cal_month, 
  cal.cal_month_name,
  cal.cal_quarter
ORDER BY cal.cal_year, cal.cal_month;

-- COMMAND ----------

-- Ventes par région géographique
SELECT 
  g.geo_country_region,
  g.geo_state_province,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  COUNT(DISTINCT f._tf_dim_customer_sk) AS distinct_customers,
  SUM(f.sales_order_qty) AS total_quantity,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_line_total) AS avg_order_line_value
FROM gold.fact_sales f
INNER JOIN gold.dim_geography g
  ON f._tf_dim_geography_sk = g._tf_dim_geography_sk
  AND g._tf_is_current = TRUE
WHERE f._tf_dim_geography_sk != -9
GROUP BY g.geo_country_region, g.geo_state_province
ORDER BY total_revenue DESC
LIMIT 20;

-- COMMAND ----------

-- Analyse des remises
SELECT 
  CASE 
    WHEN sales_unit_price_discount = 0 THEN 'No Discount'
    WHEN sales_unit_price_discount <= 0.05 THEN '1-5% Discount'
    WHEN sales_unit_price_discount <= 0.10 THEN '6-10% Discount'
    WHEN sales_unit_price_discount <= 0.20 THEN '11-20% Discount'
    ELSE '20%+ Discount'
  END AS discount_range,
  COUNT(*) AS transaction_count,
  SUM(sales_order_qty) AS total_quantity,
  SUM(sales_line_total) AS total_revenue,
  AVG(sales_unit_price) AS avg_unit_price,
  AVG(sales_unit_price_discount) AS avg_discount_rate
FROM gold.fact_sales
GROUP BY 
  CASE 
    WHEN sales_unit_price_discount = 0 THEN 'No Discount'
    WHEN sales_unit_price_discount <= 0.05 THEN '1-5% Discount'
    WHEN sales_unit_price_discount <= 0.10 THEN '6-10% Discount'
    WHEN sales_unit_price_discount <= 0.20 THEN '11-20% Discount'
    ELSE '20%+ Discount'
  END
ORDER BY 
  CASE discount_range
    WHEN 'No Discount' THEN 1
    WHEN '1-5% Discount' THEN 2
    WHEN '6-10% Discount' THEN 3
    WHEN '11-20% Discount' THEN 4
    ELSE 5
  END;

-- COMMAND ----------

-- Performance de ventes par gamme de prix des produits
SELECT 
  p.prod_price_range,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  COUNT(DISTINCT p.prod_product_id) AS distinct_products,
  SUM(f.sales_order_qty) AS total_quantity_sold,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_line_total) AS avg_line_value
FROM gold.fact_sales f
INNER JOIN gold.dim_product p
  ON f._tf_dim_product_sk = p._tf_dim_product_sk
  AND p._tf_is_current = TRUE
WHERE f._tf_dim_product_sk != -9
GROUP BY p.prod_price_range
ORDER BY 
  CASE p.prod_price_range
    WHEN 'Budget (< 100)' THEN 1
    WHEN 'Mid-Range (100-500)' THEN 2
    WHEN 'Premium (500-1000)' THEN 3
    WHEN 'Luxury (1000-3000)' THEN 4
    WHEN 'Ultra-Luxury (3000+)' THEN 5
    ELSE 6
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimisation de fact_sales

-- COMMAND ----------

-- Optimiser avec Z-ORDER sur les clés de dimension fréquemment utilisées
OPTIMIZE gold.fact_sales 
ZORDER BY (_tf_dim_calendar_sk, _tf_dim_customer_sk, _tf_dim_geography_sk, _tf_dim_product_sk);

-- COMMAND ----------

-- Nettoyer les anciennes versions (garder 7 jours)
VACUUM gold.fact_sales RETAIN 168 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Statistiques finales

-- COMMAND ----------

SELECT 
  'fact_sales' AS table_name,
  COUNT(*) AS row_count,
  COUNT(DISTINCT sales_order_id) AS distinct_orders,
  COUNT(DISTINCT _tf_dim_customer_sk) AS distinct_customers,
  COUNT(DISTINCT _tf_dim_product_sk) AS distinct_products,
  ROUND(SUM(sales_line_total), 2) AS total_sales_amount,
  ROUND(AVG(sales_line_total), 2) AS avg_line_amount,
  MIN(_tf_create_date) AS first_load_date,
  MAX(_tf_create_date) AS last_load_date
FROM gold.fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Vues agrégées pour le reporting (optionnel)

-- COMMAND ----------

-- Vue: Ventes mensuelles
CREATE OR REPLACE VIEW gold.v_sales_monthly AS
SELECT 
  cal.cal_year,
  cal.cal_month,
  cal.cal_month_name,
  cal.cal_quarter,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  SUM(f.sales_order_qty) AS total_quantity,
  SUM(f.sales_line_total) AS total_revenue,
  COUNT(DISTINCT f._tf_dim_customer_sk) AS distinct_customers,
  COUNT(DISTINCT f._tf_dim_product_sk) AS distinct_products
FROM gold.fact_sales f
INNER JOIN gold.dim_calendar cal
  ON f._tf_dim_calendar_sk = cal._tf_dim_calendar_sk
GROUP BY 
  cal.cal_year, 
  cal.cal_month, 
  cal.cal_month_name,
  cal.cal_quarter;

-- COMMAND ----------

-- Vue: Ventes par catégorie
CREATE OR REPLACE VIEW gold.v_sales_by_category AS
SELECT 
  pc.prod_cat_level,
  pc.prod_cat_hierarchy_path,
  pc.prod_cat_name,
  COUNT(DISTINCT f.sales_order_id) AS order_count,
  SUM(f.sales_order_qty) AS total_quantity,
  SUM(f.sales_line_total) AS total_revenue,
  COUNT(DISTINCT f._tf_dim_customer_sk) AS distinct_customers
FROM gold.fact_sales f
INNER JOIN gold.dim_product p
  ON f._tf_dim_product_sk = p._tf_dim_product_sk
  AND p._tf_is_current = TRUE
INNER JOIN gold.dim_product_category pc
  ON p._tf_dim_product_category_sk = pc._tf_dim_product_category_sk
  AND pc._tf_is_current = TRUE
WHERE f._tf_dim_product_sk != -9
GROUP BY 
  pc.prod_cat_level,
  pc.prod_cat_hierarchy_path,
  pc.prod_cat_name;

-- COMMAND ----------

-- Vue: Performance des clients
CREATE OR REPLACE VIEW gold.v_customer_performance AS
SELECT 
  c.cust_customer_id,
  c.cust_full_name,
  c.cust_company_name,
  c.cust_email_address,
  COUNT(DISTINCT f.sales_order_id) AS total_orders,
  SUM(f.sales_order_qty) AS total_items_purchased,
  SUM(f.sales_line_total) AS total_revenue,
  AVG(f.sales_line_total) AS avg_order_line_value,
  MIN(cal.cal_date) AS first_purchase_date,
  MAX(cal.cal_date) AS last_purchase_date,
  DATEDIFF(MAX(cal.cal_date), MIN(cal.cal_date)) AS customer_lifetime_days
FROM gold.fact_sales f
INNER JOIN gold.dim_customer c
  ON f._tf_dim_customer_sk = c._tf_dim_customer_sk
  AND c._tf_is_current = TRUE
INNER JOIN gold.dim_calendar cal
  ON f._tf_dim_calendar_sk = cal._tf_dim_calendar_sk
WHERE f._tf_dim_customer_sk != -9
GROUP BY 
  c.cust_customer_id,
  c.cust_full_name,
  c.cust_company_name,
  c.cust_email_address;
