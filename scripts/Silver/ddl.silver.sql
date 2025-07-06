/*
============================================================
** DDL Sript : Create Silver Tables**
============================================================
SCRIPT PURPOSE :
This scripr create tables i 'Silve'r Schema droppin the Tables 
if they are already exists,
Run this script to redefine structure of the bronze tables 
============================================================
*/



IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
 DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR (50),
	cst_creatadate DATE,
	dwh_create_date  DATETIME2 DEFAULT GETDATE()
	);
--All above columns will come from source sysytem.only last one will come from the data engineer side i.e. Meta Data Columns.
--DEFAULT GETDATE() ensures the column gets the current timestamp automatically (.ie. by default) when no value is explicitly given during inserts 


IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
 DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR (50),--> [UPDATED and ADDED Column After trasnfomation on bronze.crm.prd_info table]
	prd_key NVARCHAR(50),--> [Same column but data in this got trasnfromed After trasnfomation on bronze.crm.prd_info table]
	prd_name NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_date DATE,--> [UPDATED data type to date after transfomration]
	prd_end_date DATE,--> [UPDATED data type to date after transfomration]
	dwh_create_date  DATETIME2 DEFAULT GETDATE()
	);
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
 DROP TABLE  silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prod_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,    --> UPDATED: these columns value from INT to DATE cause data transfomation is done, so need to update them
	sls_ship_date DATE,   --> UPDATED: these columns value from INT to DATE cause data transfomation is done, so need to update them 
	sls_due_date DATE,    --> UPDATED: these columns value from INT to DATE cause data transfomation is done, so need to update them
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date  DATETIME2 DEFAULT GETDATE()
	);

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
 DROP TABLE  silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date  DATETIME2 DEFAULT GETDATE()
	);


IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
 DROP TABLE   silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date  DATETIME2 DEFAULT GETDATE()
	);


IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
 DROP TABLE   silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50), 
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date  DATETIME2 DEFAULT GETDATE()
	);
