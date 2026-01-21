/*
================================================================================
Procedure Name : [silver].[load_silver]
Layer          : Silver (Medallion Architecture)
Author         : (your name / team)
Created Date   : (date)
Last Updated   : (date)

Purpose:
    Loads and transforms data from the Bronze layer into the Silver layer.
    This procedure cleans, standardizes, and fixes common data quality issues
    before storing the results in curated Silver tables.

Source Tables (Bronze):
    - bronze.crm_cust_info
    - bronze.crm_sales_details
    - bronze.erp_cust_az12
    - bronze.erp_loc_a101
    - bronze.erp_px_cat_g1v2

Target Tables (Silver):
    - silver.crm_cust_info
    - silver.crm_sales_details
    - silver.erp_cust_az12
    - silver.erp_loc_a101
    - silver.erp_px_cat_g1v2

Key Transformations / Rules:
    CRM Customers:
        - Trims names and removes extra spaces
        - Converts marital status codes (M/S) into readable values
        - Converts gender codes (M/F) into readable values
        - Keeps only the latest record per customer (ROW_NUMBER by create_date)

    CRM Sales:
        - Converts numeric YYYYMMDD date values into DATE format
        - Sets invalid dates (0 or not length 8) to NULL
        - Fixes sales amount when missing/invalid by recalculating:
              sales = quantity * ABS(price)
        - Fixes price when missing/invalid using:
              price = sales / quantity (NULLIF prevents divide by zero)

    ERP Customer:
        - Removes "NAS" prefix from cid when present
        - Sets future birthdates to NULL
        - Standardizes gender values into Male/Female/n/a

    ERP Location:
        - Removes hyphens from cid
        - Expands country codes (DE, US/USA)
        - Handles missing country values as 'n/a'

Load Strategy:
    - Full refresh (TRUNCATE + INSERT) for each Silver table

Logging:
    - Prints progress messages
    - Tracks duration per table load and total batch duration

Error Handling:
    - Uses TRY/CATCH
    - Prints error details (message, number, state)

Notes / Considerations:
    - TRUNCATE requires appropriate permissions
    - This procedure is not incremental (rebuilds Silver each run)
================================================================================
*/
CREATE OR ALTER PROCEDURE [silver].[load_silver]
AS
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @batch_start_time DATETIME, 
            @batch_end_time DATETIME;

    BEGIN TRY
        PRINT '========================================================';
        PRINT 'Loading Silver Layer';
        PRINT '========================================================';

        SET @batch_start_time = GETDATE();

        PRINT '--------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------------------------';

        -------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncate Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
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
            CASE                                                
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

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        -------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncate Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
            CASE 
                WHEN sls_order_date = 0 OR LEN(sls_order_date) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE)
            END AS sls_order_date,
            CASE 
                WHEN sls_ship_date = 0 OR LEN(sls_ship_date) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE)
            END AS sls_ship_date,
            CASE 
                WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
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
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';


        PRINT '--------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------------------------';

        -------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncate Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, birthdate, gender)
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN birthdate > GETDATE() THEN NULL
                ELSE birthdate
            END AS birthdate,
            CASE 
                WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gender
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        -------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncate Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
                ELSE TRIM(cntry) 
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        -------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncate Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, main)
        SELECT 
            id,
            cat,
            subcat,
            main
        FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        -------------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '>> Batch Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

    END TRY
    BEGIN CATCH
        PRINT '=========================================================';
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';
    END CATCH
END;
