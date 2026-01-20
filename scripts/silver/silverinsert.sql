
-----------------------------------------------------------------------------------------
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,           
    TRIM(cst_lastname) AS cst_lastname,             
    CASE                                            
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE                                                -
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
  
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1; 

-----------------------------------------------------------------------------------------

INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_date,
	sls_ship_date,
	sls_due_date,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_date = 0 OR LEN(sls_order_date) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE)
END AS sls_order_date,
CASE WHEN sls_ship_date = 0 OR LEN(sls_ship_date) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE)
END AS sls_ship_date,
CASE WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE)
END AS sls_due_date,
CASE 
	WHEN CAST(sls_sales AS INT) IS NULL OR CAST(sls_sales AS INT) <= 0 
	OR CAST(sls_sales AS INT) != CAST(sls_quantity AS INT) * ABS(CAST(sls_price AS INT))
	THEN CAST(sls_quantity AS INT) * ABS(CAST(sls_price AS INT))
	ELSE CAST(sls_sales AS INT) 
END AS sls_sales,
CAST(sls_quantity AS INT) AS sls_quantity,
CASE 
	WHEN CAST(sls_price AS INT) IS NULL OR CAST(sls_price AS INT) <= 0
	THEN CAST(sls_sales AS INT) / NULLIF(CAST(sls_quantity AS INT), 0)
	ELSE CAST(sls_price AS INT) 
END AS sls_price
FROM bronze.crm_sales_details
-----------------------------------------------------------------------------------------