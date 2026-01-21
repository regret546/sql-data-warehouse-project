
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-- Data Standardization & Consistency
SELECT DISTINC cst_marital_status 
FROM bronze.crm_cust_info

-- Check for Invalid Orders
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_end_date < prd_start_date

-- Check for NULLS or Negative Numbers
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost is NULL

-- Check for Invalid Dates
SELECT 
NULLIF(sls_order_date, 0) sls_order_date
FROM silver.crm_sales_details
WHERE sls_order_date <=0
OR LEN(sls_order_date) != 8
OR sls_order_date > 20500101
OR sls_order_date < 19000101

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_date > sls_ship_date 
OR sls_order_date >  sls_due_date

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative

SELECT DISTINCT
	CAST(sls_sales AS INT) AS old_sls_sales,
	CASE 
		WHEN CAST(sls_sales AS INT) IS NULL OR CAST(sls_sales AS INT) <= 0 
		OR CAST(sls_sales AS INT) != CAST(sls_quantity AS INT) * ABS(CAST(sls_price AS INT))
		THEN CAST(sls_quantity AS INT) * ABS(CAST(sls_price AS INT))
		ELSE CAST(sls_sales AS INT) 
	END AS sls_sales,
	CAST(sls_quantity AS INT) AS sls_quantity,
	CAST(sls_price AS INT) AS old_sls_price,
	CASE 
		WHEN CAST(sls_price AS INT) IS NULL OR CAST(sls_price AS INT) <= 0
		THEN CAST(sls_sales AS INT) / NULLIF(CAST(sls_quantity AS INT), 0)
		ELSE CAST(sls_price AS INT) 
	END AS sls_price
FROM bronze.crm_sales_details
WHERE CAST(sls_quantity AS INT) * CAST(sls_price AS INT)
!= CAST(sls_sales AS INT)
OR CAST(sls_sales AS INT) IS NULL OR CAST(sls_quantity AS INT) IS NULL
OR CAST(sls_price AS INT) IS NULL OR CAST(sls_sales AS INT) <= 0 OR
CAST(sls_quantity AS INT) <= 0 OR CAST(sls_price AS INT) <= 0
ORDER BY CAST(sls_sales AS INT), CAST(sls_quantity AS INT), CAST(sls_price AS INT)

-- Identify Out-of-Range Dates
SELECT DISTINCT
	birthdate
FROM bronze.erp_cust_az12
WHERE birthdate < '1924-01-01' OR birthdate > GETDATE()

-- Data Standardization and Consistency
SELECT DISTINCT
	gender,
	CASE 
		WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END
FROM bronze.erp_cust_az12

-- Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

-- Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE main != TRIM(main)

-- Data Standardization & Consistency
SELECT DISTINCT main
FROM bronze.erp_px_cat_g1v2 