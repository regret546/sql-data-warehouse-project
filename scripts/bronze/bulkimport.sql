/*
================================================================================
Script: Bronze Layer Data Load (CRM + ERP)
Database: DataWarehouse

Purpose:
This script loads raw source data from CSV files into the BRONZE staging tables.
It first truncates each target table to remove any existing records, then uses
BULK INSERT to import fresh data from the CRM and ERP dataset folders.

Process Summary:
1) TRUNCATE TABLE to clear old data (full refresh load)
2) BULK INSERT from local CSV files (skipping headers using FIRSTROW = 2)

Tables Loaded:
- bronze.crm_cust_info         (cust_info.csv)
- bronze.crm_prd_info          (prd_info.csv)
- bronze.crm_sales_details     (sales_details.csv)
- bronze.erp_cust_az12         (CUST_AZ12.csv)
- bronze.erp_loc_a101          (LOC_A101.csv)
- bronze.erp_px_cat_g1v2       (PX_CAT_G1V2.csv)

Notes:
- TRUNCATE will remove all data from the tables, so use with caution.
- File paths must be accessible by the SQL Server service account.
- FIELDTERMINATOR is set to comma (CSV format).
- TABLOCK is used to improve load performance by applying a table-level lock.
================================================================================
*/


USE DataWarehouse

TRUNCATE TABLE bronze.crm_cust_info;

BULK INSERT bronze.crm_cust_info
FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2, -- Where the data starts
	FIELDTERMINATOR = ',', -- Delimiter or Separator
	TABLOCK -- hint in SQL Server is a query optimizer instruction that forces a table-level 
			-- lock for the duration of the statement or transaction.
);

TRUNCATE TABLE bronze.crm_prd_info;

BULK INSERT bronze.crm_prd_info
FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2, -- Where the data starts
	FIELDTERMINATOR = ',', -- Delimiter or Separator
	TABLOCK -- hint in SQL Server is a query optimizer instruction that forces a table-level 
			-- lock for the duration of the statement or transaction.
);

TRUNCATE TABLE bronze.crm_sales_details;

BULK INSERT bronze.crm_sales_details
FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2, -- Where the data starts
	FIELDTERMINATOR = ',', -- Delimiter or Separator
	TABLOCK -- hint in SQL Server is a query optimizer instruction that forces a table-level 
			-- lock for the duration of the statement or transaction.
);

TRUNCATE TABLE bronze.erp_cust_az12;

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2, -- Where the data starts
	FIELDTERMINATOR = ',', -- Delimiter or Separator
	TABLOCK -- hint in SQL Server is a query optimizer instruction that forces a table-level 
			-- lock for the duration of the statement or transaction.
);

TRUNCATE TABLE bronze.erp_loc_a101;

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2, -- Where the data starts
	FIELDTERMINATOR = ',', -- Delimiter or Separator
	TABLOCK -- hint in SQL Server is a query optimizer instruction that forces a table-level 
			-- lock for the duration of the statement or transaction.
);


TRUNCATE TABLE bronze.erp_px_cat_g1v2;

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2, -- Where the data starts
	FIELDTERMINATOR = ',', -- Delimiter or Separator
	TABLOCK -- hint in SQL Server is a query optimizer instruction that forces a table-level 
			-- lock for the duration of the statement or transaction.
);
