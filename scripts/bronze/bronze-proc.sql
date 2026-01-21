
/*
================================================================================
Stored Procedure: bronze.load_broze
Database: DataWarehouse
Layer: Bronze (Raw/Staging)

Purpose:
This stored procedure performs a full refresh load of the Bronze layer by:
1) Truncating existing Bronze tables (clearing old data)
2) Reloading fresh data from CRM and ERP CSV source files using BULK INSERT

What it does:
- Loads CRM tables:
  - bronze.crm_cust_info
  - bronze.crm_prd_info
  - bronze.crm_sales_details
- Loads ERP tables:
  - bronze.erp_cust_az12
  - bronze.erp_loc_a101
  - bronze.erp_px_cat_g1v2

Logging & Monitoring:
- Prints progress messages for each table load
- Captures and prints load duration per table
- Captures and prints total batch duration for the entire Bronze load

Error Handling:
- Uses TRY...CATCH to catch load failures and print SQL error details
  (message, error number, and state) for troubleshooting.

Important Notes:
- This procedure will DELETE all existing data in the Bronze tables via TRUNCATE.
- CSV file paths must be accessible by the SQL Server service account.
- Designed for batch execution (e.g., scheduled job / ETL pipeline run).
================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_broze
AS
BEGIN
   DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
   BEGIN TRY
    PRINT '========================================================';
    PRINT 'Loading Bronze Layer';
    PRINT '========================================================';

    PRINT '--------------------------------------------------------';
    PRINT 'Loading CRM Table';
    PRINT '--------------------------------------------------------';

    SET @batch_start_time = GETDATE();
    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> Inserting Data Into: bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT '>> Inserting Data Into: bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';


    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT '>> Inserting Data Into: bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';

    PRINT '--------------------------------------------------------';
    PRINT 'Loading ERP Table';
    PRINT '--------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';

    SET @start_time = GETDATE();
    PRINT '>> Truncate Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';

    SET @batch_end_time = GETDATE();
    PRINT '>> Batch Total Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> --------------------------';

       END TRY
       BEGIN CATCH
            PRINT '=========================================================';
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
            PRINT 'Error Message' + ERROR_MESSAGE();
            PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
            PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
            PRINT '=========================================================';
       END CATCH
END;

