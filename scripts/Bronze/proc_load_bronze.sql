==================================================================
**Stored Procedure : Load Bronze Layer (source-> Bronze)**
==================================================================
Script Purpose:
______________ This Stored Procedure Load data into Bronze layer From External Sources.
##It Performs Following Actions:##
Truncate the Bronze layer Tables before loadind data.
Use 'Bulk Insert' command to load data from .CRV source to Bronze Layer.
This Stored Procedure doesnot accept any Parameter or return any values.
Usage Example:
EXEC bronze.load_bronze;

===================================================================


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time =	GETDATE();
	    PRINT'==============================================================';
		PRINT'Loading Bronze Layer';
		PRINT'==============================================================';
	  
		PRINT'--------------------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'--------------------------------------------------------------';
	    SET @start_time =GETDATE();
		PRINT'>> Truncate Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> Inserting data into Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\psran\OneDrive\Documents\SQl-Data-Warehouse-Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (FIRSTROW = 2,
			  FIELDTERMINATOR =',',
			  TABLOCK
			  );
        SET @end_time =GETDATE();
        PRINT'>>Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS Nvarchar) + 'Seconds' ;  
		PRINT'-----------------------------------';
         
		SET @start_time =GETDATE();
		PRINT'>> Truncate Table: bronze.crm_prd_info';
		TRUNCATE TABLE  bronze.crm_prd_info;

		PRINT'>> Inserting data into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\psran\OneDrive\Documents\SQl-Data-Warehouse-Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( FIRSTROW = 2,
		FIELDTERMINATOR =',',
		TABLOCK );
		SET @end_time =GETDATE();
        PRINT'>>Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS Nvarchar) + 'Seconds' ;  
		PRINT'-----------------------------------';
		
		SET @start_time =GETDATE();
		PRINT'>> Truncate Table: bronze.crm_sales_details';
		TRUNCATE TABLE  bronze.crm_sales_details;

		PRINT'>> Inserting data into Table: bronze.crm_sales_details';
		BULK INSERT  bronze.crm_sales_details
		FROM 'C:\Users\psran\OneDrive\Documents\SQl-Data-Warehouse-Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH ( FIRSTROW = 2,
		FIELDTERMINATOR =',',
		TABLOCK );
	    SET @end_time =GETDATE();
		PRINT'>>Load Duration:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS Nvarchar) + 'Seconds' ;  
		PRINT'-----------------------------------';


		PRINT'--------------------------------------------------------------';
		PRINT'Lading ERP Tables';
		PRINT'--------------------------------------------------------------';

		SET @start_time =GETDATE();
		PRINT'>> Truncate Table: bronze.erp_cust_az12';
		TRUNCATE TABLE  bronze.erp_cust_az12;

		PRINT'>> Inserting data into Table: bronze.erp_cust_az12';
		BULK INSERT  bronze.erp_cust_az12
		FROM 'C:\Users\psran\OneDrive\Documents\SQl-Data-Warehouse-Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH ( FIRSTROW = 2,
		FIELDTERMINATOR =',',
		TABLOCK );
	    SET @end_time =GETDATE();
		PRINT'>>Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS Nvarchar) + 'Seconds' ;  
		PRINT'-----------------------------------';

        SET @start_time =GETDATE();
		PRINT'>> Truncate Table: bronze.erp_loc_a101';
		TRUNCATE TABLE   bronze.erp_loc_a101;

		PRINT'>> Inserting data into Table: bronze.erp_loc_a101';
		BULK INSERT  bronze.erp_loc_a101
		FROM 'C:\Users\psran\OneDrive\Documents\SQl-Data-Warehouse-Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH ( FIRSTROW = 2,
		FIELDTERMINATOR =',',
		TABLOCK );
		SET @end_time =GETDATE();
	    PRINT'>>Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS Nvarchar) + 'Seconds' ;  
		PRINT'-----------------------------------';

		SET @start_time =GETDATE();
		PRINT'>> Truncate Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE  bronze.erp_px_cat_g1v2;

		PRINT'>> Inserting data in to Table: bronze.erp_px_cat_g1v2';
		BULK INSERT  bronze.erp_px_cat_g1v2
		FROM 'C:\Users\psran\OneDrive\Documents\SQl-Data-Warehouse-Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH ( FIRSTROW = 2,
		FIELDTERMINATOR =',',
		TABLOCK );
		SET @end_time =GETDATE();
		PRINT'>>Load Duration :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS Nvarchar) + 'Seconds' ;  
		PRINT'-----------------------------------';
		SET @batch_end_time =GETDATE();
		PRINT'=============================================================';
		PRINT'Loading Bronze Layer is Completed';
		PRINT'=============================================================';
		PRINT'  ** Total Bronze Batch Loading Time  ** : '+ CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS Nvarchar)+ 'seconds';
		PRINT'=============================================================';
	 END TRY
	 BEGIN CATCH
	   PRINT'=============================================================';
	   PRINT'Error Occured During the loadingof bronze layer';
	   PRINT'Error Message:' +Error_Message();
	   PRINT'Error Number:' + CAST(Error_Number() AS NVARCHAR );
	   PRINT'Error Number:'+ CAST(Error_State() AS NVARCHAR);
	   PRINT'===============================================================';
    END CATCH
END;

 EXEC Bronze.load_bronze;
