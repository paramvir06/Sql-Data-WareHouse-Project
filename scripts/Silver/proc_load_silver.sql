/*
=================================================================================
STORED PROCEDURE :Load  silver layer 
=================================================================================
Scripts Purpose : 
       This Stored Procedure Do ETL to load data from bronze layer tables to  The silver layer 
respectves table and poplulate them
Actions Performed :
1) Truncate Silver Tables
2)INSERT transformerd and cleansed data from bronze layer to the Silver layer

Parameters:
None, thi stored Procedure accepts any stored procedure or return any saved values

Usage Example :
EXEC silver.load_silver :
==============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME2,@end_time DATETIME2,@start_batch_time DATETIME2, @end_batch_time DATETIME2;
	BEGIN TRY
	SET @start_batch_time = GETDATE();
	PRINT'==========================================================================';
	PRINT' Loading Silver Layer :';
	PRINT'==========================================================================';

	PRINT'--------------------------------------------------------------------------';
	PRINT'Loading : CRM Tables'; 
	PRINT'--------------------------------------------------------------------------';
	SET  @start_time = GETDATE();
	PRINT '==> Truncating Table :  Silver.crm_cust_info'
	TRUNCATE TABLE Silver.crm_cust_info;
	PRINT '==> Inserting Data into Table :  Silver.crm_cust_info'
	INSERT INTO Silver.crm_cust_info(
		 cst_id,
		 cst_key,
		 cst_firstname,
		 cst_lastname,
		 cst_material_status,
		 cst_gndr,
		 cst_creatadate)
	SELECT  cst_id,cst_key,
		TRIM(cst_firstname) AS cst_firstname,--> Remove unwanted Sapce to ensure data consistency and uniformity across all the records. 
		TRIM(cst_lastname) AS cst_lastname,
			CASE
			 WHEN UPPER(TRIM(cst_material_status)) ='M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_material_status)) ='S' THEN 'Single'
			 ELSE 'Unkown'
			 END cst_material_status, --> Normalize marital status values to readable format.(Data Normalization/Standrdization) Maps coded values into more user friendly & meaningfull descriptions	
			CASE 
			 WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			 ELSE 'Unkown' --> Hndling the missing values intead of nulls 'Unkown' Will show up. 
			 END cst_gndr,
			 cst_creatadate  --Normalize gender value to readable formats (Data Normalization/Standrdization)
			 FROM
			 (SELECT * ,ROW_NUMBER()over(Partition by  cst_id ORDER BY cst_creatadate DESC) AS Flag_last
			 FROM Bronze.crm_cust_info WHERE cst_id IS NOT NULL )TT --> kept it IS NOT NULL, then Null record with falg value 1 wont show up. as we dont have null in primary keys.
			 WHERE TT.Flag_last = 1;
			 SET @end_time = GETDATE();
			 PRINT'Load Duration : ' + CAST(DATEDIFF (second,@start_time,@end_time)AS NVARCHAR) +' Seconds';
			 PRINT'------------------------------------------------------------------------';
	--table:2
	SET @start_time =GETDATE();
	PRINT '==> Truncating Table : silver.crm_prd_info'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '==> Insert into Table : silver.crm_prd_info'
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id ,-->(Derived Column) [UPDATED and ADDED Column After trasnfomation on bronze.crm.prd_info table]
		prd_key ,--> (Derived Column)[Same column but data in this got trasnfromed After trasnfomation on bronze.crm.prd_info table]
		prd_name ,
		prd_cost ,
		prd_line ,
		prd_start_date ,--> [UPDATED data type to date after transfomration]
		prd_end_date 
		)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,--> [used LEN(prd_key) instead of number col size of every record iv changnig so keep the range dynamic we hvae used lenght LEN()]
		prd_name,
		ISNULL( prd_cost,0) AS prd_cost,
		CASE  UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mounatin'
		WHEN 'R' THEN 'Road' 
		WHEN 'S' THEN 'Other Sales' 
		WHEN 'T' THEN 'Touring' 
		ELSE 'Unkown'
		END prd_line,-->Map Product line code into desciptive values
		CAST(prd_start_date AS DATE) AS prd_start_date,
		CAST(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)-1 AS date) AS prd_end_date --> (Data Enrichment)calculate end day  as one day before the next start day	   
		FROM Bronze.crm_prd_info;
		SET @end_time = GETDATE();
			 PRINT'Load Duration : ' + CAST(DATEDIFF (second,@start_time,@end_time)AS NVARCHAR) +'Seconds';
			 PRINT'------------------------------------------------------------------------';
	--table:3
	SET @start_time =GETDATE();
	PRINT '==> Truncating Table : silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '==> Insert into Table : silver.crm_sales_details'
	INSERT INTO silver.crm_sales_details(
		sls_ord_num ,
		sls_prod_key ,
		sls_cust_id ,
		sls_order_dt ,    
		sls_ship_date ,   
		sls_due_date ,    
		sls_sales ,
		sls_quantity ,
		sls_price 
		 )
	SELECT
		sls_ord_num,sls_prod_key,
		sls_cust_id,
		CASE--> checknig data quality
		WHEN LEN(sls_order_dt) != 8 OR LEN(sls_order_dt) = 0 THEN NULL --> Handling invalid Data 
		ELSE CAST(CAST(sls_order_dt AS varchar) AS date)--> [Data type Casting]orderdate in now real date insteasd of text. you can see output of this query
		END AS sls_order_dt,
		CASE
		WHEN LEN(sls_ship_date) != 8 OR LEN(sls_order_dt) = 0 THEN NULL
		ELSE CAST(CAST(sls_ship_date AS varchar) AS date)
		END AS sls_ship_date,
		CASE
		WHEN LEN(sls_due_date) != 8 OR LEN(sls_due_date) = 0 THEN NULL
		ELSE CAST(CAST(sls_due_date AS varchar)AS DATE)
		END AS sls_due_date,
		CASE--> Checking business rules
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity* ABS(sls_price) 
			THEN sls_quantity* ABS(sls_price)
		ELSE sls_sales 
		END AS sls_sales, --> Recalculating sales if orignal value is missing or inccorect
		sls_quantity,
		CASE
		WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales/NULLIF(sls_quantity,0) --> used null if so if there is zero in quantity then error will come to avoid that we did this
		ELSE sls_price
		END sls_price   --> Drive Price if orignal values are invalid
		FROM Bronze.crm_sales_details;
		SET @end_time = GETDATE();
			 PRINT'Load Duration : ' + CAST(DATEDIFF (second,@start_time,@end_time)AS NVARCHAR) + 'Seconds';
			 PRINT'------------------------------------------------------------------------';
	--table:4
	SET @start_time =GETDATE();
	PRINT '==> Truncating Table : silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '==> Insert into Table : silver.erp_cust_az12'
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen 
		)
	SELECT
		CASE 
			WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE')   THEN 'Male'
			 ELSE 'Unkonwn'
		END AS gen
		FROM Bronze.erp_cust_az12;
		SET @end_time = GETDATE();
			 PRINT'Load Duration : ' + CAST(DATEDIFF (second,@start_time,@end_time)AS NVARCHAR) + 'Seconds';
			 PRINT'------------------------------------------------------------------------';
	--table:5
	SET @start_time =GETDATE();
	PRINT '==> Truncating Table : silver.erp_loc_a101'
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '==>  Insert into Table : silver.erp_loc_a101'
	INSERT INTO silver.erp_loc_a101 (
		cid ,
		cntry 
		)
	SELECT REPLACE(cid,'-','')cid,
		CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IN('US','USA') THEN 'United States'
			 WHEN UPPER(TRIM(cntry)) IS NULL OR	UPPER(TRIM(cntry)) =' ' THEN 'Unkown'
			 ELSE trim(cntry)  
			 END AS Country
		FROM Bronze.erp_loc_a101;
		SET @end_time = GETDATE();
			 PRINT'Load Duration : ' + CAST(DATEDIFF (second,@start_time,@end_time)AS NVARCHAR) + 'Seconds';
			 PRINT'------------------------------------------------------------------------';
	--table:6
	SET @start_time =GETDATE();
	PRINT '==> Truncating Table : silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '==>  Insert into Table : silver.erp_px_cat_g1v2'
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat, 
		subcat,
		maintenance
		)
	SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM Bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
			 PRINT'Load Duration : ' + CAST(DATEDIFF (second,@start_time,@end_time)AS NVARCHAR) + 'Seconds';
			 PRINT'-----------------------------------------------------------------';
			 SET @end_batch_time = GETDATE();
			 PRINT'=================================================================';
		     PRINT'Loading silver Layer is Completed';
		     PRINT'=================================================================';
			 PRINT'  ** Total Silver Batch Loading Time  ** : '+ CAST(DATEDIFF(second, @start_batch_time, @end_batch_time) AS Nvarchar)+ 'seconds';
		PRINT'======================================================================';
		END TRY
		BEGIN CATCH
		PRINT'=======================================================================';
		PRINT'Error Occured During Execution of Silver Layer';
		PRINT'Error Message :' + ERROR_MESSAGE();
		PRINT'Error Message :' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message :' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT'=======================================================================';
		END CATCH
END;
 
