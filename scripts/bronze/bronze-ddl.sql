/* 
================================================================================
Script: Bronze Layer Table Setup (CRM + ERP)
Database: DataWarehouse

Purpose:
This script initializes the BRONZE staging tables used for ingesting raw data
from CRM and ERP source systems. It drops the existing tables (if they exist)
and recreates them with the required schema, ensuring a clean and consistent
structure before loading data.

Tables Created:
- bronze.crm_cust_info
- bronze.crm_prd_info
- bronze.crm_sales_details
- bronze.erp_cust_az12
- bronze.erp_loc_a101
- bronze.erp_px_cat_g1v2

Note:
Running this script will delete existing data in these tables.
================================================================================
*/

USE DataWarehouse

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE
)

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_date DATETIME,
	prd_end_date DATETIME
)

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id NVARCHAR(50),
	sls_order_date NVARCHAR(50),
	sls_ship_date NVARCHAR(50),
	sls_due_date NVARCHAR(50),
	sls_sales NVARCHAR(50),
	sls_quantity NVARCHAR(50),
	sls_price NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12 ;
CREATE TABLE bronze.erp_cust_az12 (
	CID NVARCHAR(50),
	Birthdate DATE,
	Gender NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101 ;
CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2 ;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	main NVARCHAR (50)
)