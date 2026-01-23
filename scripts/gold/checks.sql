/* 
=========================================================
DATA QUALITY / VALIDATION QUERIES (GOLD LAYER PRE-CHECKS)
=========================================================
These queries are used to validate data consistency before
or after building Gold layer dimensions and facts.

1) Gender Standardization Check
   - Compares CRM gender vs ERP gender values.
   - CRM is treated as the master source when available.
   - If CRM is 'n/a', fallback to ERP gender.
   - If both are missing, default to 'n/a'.

2) Join Validation Example
   - Confirms that the joining key correctly matches between
     two datasets (CTE and base table).

3) Fact Foreign Key Integrity Checks
   - Ensures every surrogate key in the fact table exists in
     the corresponding dimension table.
   - Any NULL results mean the fact record has a missing or
     unmatched dimension reference (broken relationship).
=========================================================
*/

-- 1) GENDER STANDARDIZATION CHECK (CRM is master, ERP is fallback)
SELECT DISTINCT
	ci.cst_gndr,
	ca.gender,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the Master for gender info
		ELSE COALESCE(ca.gender, 'n/a')             -- Fallback to ERP, else default 'n/a'
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
ORDER BY 1,2;


-- 2) JOIN VALIDATION EXAMPLE (Confirm employee_id matches correctly)
SELECT 
	q.unique_query,
	q.employee_id
FROM cte_unique_query q
INNER JOIN employees e
	ON q.employee_id = e.employee_id;


-- 3) FACT CHECK: FOREIGN KEY INTEGRITY (DIMENSIONS)
-- Check for missing customer dimension records referenced by fact_sales
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;


-- Check for missing product dimension records referenced by fact_sales
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
WHERE p.product_key IS NULL;
