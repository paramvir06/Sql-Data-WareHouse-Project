/*
===============================================================================================================
                                            Quality Checks
==============================================================================================================
Script Prupose:
       This Script chechks Various quality check for data consistency,data quality,accuracy
       and standardization across silver schemas. It Inculdes chechk for :
       - Null or Duplicate Primary key.
       - Unwanted space in Strings Fields .
       - Data Standardization and Consistency.
       - Invalid data range and orders.

Usage Notes :
- Use the scipts only after data loadin in silver layer
- Invstigate and resolve if any discrepancies found during data checks

================================================================================================================
*/

--=======================================================
--Quality Check For Silver LAyer FOR silver.crm_cust_info
--=======================================================
--query:1 Check for Duplicate or null in Primary Key
--Expectatin : No results
	SELECT cst_id,COUNT(*)
	FROM silver.crm_cust_info
	GROUP BY cst_id
	HAVING COUNT(*) >1 OR cst_id IS NULL ;

--query:2 Window function to show duplicates in  silver layer tables
--Expectatin : No results
SELECT * FROM(SELECT * ,ROW_NUMBER()over(Partition by  cst_id ORDER BY cst_creatadate DESC) AS Flag_last
				  FROM silver.crm_cust_info)TT
				  WHERE TT.Flag_last != 1;

--query:3 check for unwanted sapces
--Expectatin : No results
	SELECT cst_firstname 
	FROM silver.crm_cust_info
	WHERE cst_firstname !=TRIM(cst_firstname);

	SELECT cst_firstname 
	FROM silver.crm_cust_info
	WHERE cst_lastname !=TRIM(cst_lastname);

--query 4:Data standardization & Consistency

SELECT DISTINCT cst_gndr 
 FROM silver.crm_cust_info;

 SELECT * FROM Silver.crm_cust_info;

 --=======================================================
--Quality Check For Silver LAyer FOR silver.crm_prd_info
--=======================================================
--query:1 Check for Duplicate or null in Primary Key
--Expectatin : No results
	SELECT prd_id,COUNT(*)
	FROM silver.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) >1 OR prd_id IS NULL ;

--query:2 check for unwanted sapces
--Expectatin : No results
	SELECT prd_name 
	FROM silver.crm_prd_info
	WHERE prd_name !=TRIM(prd_name);

--query:3 For Null & Negative values
--Expectatin : No results
SELECT * 
	FROM Silver.crm_prd_info
	WHERE prd_cost IS NULL OR prd_cost < 0;

--query 4:Data standardization & Consistency

SELECT DISTINCT  prd_line
 FROM silver.crm_prd_info;


--query 5:Check for invalid date order
SELECT * 
	FROM Silver.crm_prd_info
	WHERE prd_start_date > prd_end_date;

 SELECT * FROM Silver.crm_prd_info;

  --=======================================================
--Quality Check For Silver LAyer FOR silver.crm_sales_datails
--=======================================================
--Chek for Inavalid Order Date
-- Expecatation No Reults
SELECT 
	*
	FROM Silver.crm_sales_details
	WHERE sls_order_dt > sls_ship_date OR sls_order_dt >sls_due_date ;

--Check Data Consistency Between Sales, Order And Qiantity
--Expectation: No reults
SELECT 
	sls_sales ,
	sls_quantity,
	sls_price 
    FROM Silver.crm_sales_details
	WHERE (sls_price)*(sls_quantity) != sls_sales                 
	OR sls_quantity IS NULL OR sls_sales IS NULL OR sls_price IS NULL
	OR sls_quantity <= 0 OR sls_sales  <= 0  OR sls_price  <= 0      
	ORDER BY sls_sales,sls_quantity,sls_price;
--==> Final look at the table
SELECT * FROM Silver.crm_sales_details;		

  --=======================================================
--Quality Check For Silver LAyer FOR silver.erp_cust_az12
--=======================================================

--Identify Out of range Dates

SELECT 
	bdate
	FROM Silver.erp_cust_az12
	WHERE bdate < '1924-01-01' OR bdate >GETDATE()

-- Data Standardization And Consistency
SELECT 
	 DISTINCT GEN
	FROM Silver.erp_cust_az12;

SELECT * FROM Silver.erp_cust_az12;

  --=======================================================
--Quality Check For Silver LAyer FOR silver.erp_loc_a101
--=======================================================

-- Data Standardization And Consistency
SELECT 
	DISTINCT cntry
	FROM Silver.erp_loc_a101;

SELECT * FROM Silver.erp_loc_a101;

