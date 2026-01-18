/* 
================================================================================
Script: Silver Layer Table Setup (CRM + ERP)
Database: DataWarehouse

Purpose:
This script creates the SILVER layer tables used to store cleaned and structured
data from the CRM and ERP sources. It drops existing Silver tables (if present)
and recreates them with the defined schema, including a DWH audit column
(dwh_create_date) to track when records are loaded into the warehouse.

Tables Created:
- silver.crm_cust_info
- silver.crm_prd_info
- silver.crm_sales_details
- silver.erp_cust_az12
- silver.erp_loc_a101
- silver.erp_px_cat_g1v2

Notes:
- Running this script will remove and recreate the tables, deleting any existing data.
- The dwh_create_date column is automatically populated using GETDATE() on insert.
================================================================================
*/

USE DataWarehouse

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
    cat_id,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_date DATE,
	prd_end_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_date NVARCHAR(50),
	sls_ship_date NVARCHAR(50),
	sls_due_date NVARCHAR(50),
	sls_sales NVARCHAR(50),
	sls_quantity NVARCHAR(50),
	sls_price NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12 ;
CREATE TABLE silver.erp_cust_az12 (
	CID NVARCHAR(50),
	Birthdate DATE,
	Gender NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101 ;
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2 ;
CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	main NVARCHAR (50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)