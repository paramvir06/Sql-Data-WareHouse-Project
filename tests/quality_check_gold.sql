-- Checking the aggregation that has been done on the gender column of both crm n erp customer information table

SELECT DISTINCT gender FROM gold.dim_customer;

--Qulaity check of view gold.facts_sale. Foriegn key ingerity (dimensions)
--------------------------------------------------------------------------
SELECT
	* 
	FROM 
	Gold.facts_sales AS fs
	LEFT JOIN gold.dim_customer dc
	ON fs.customer_key = dc.customer_key 
	WHERE fs.customer_key IS NULL;
-->[Fact check no output of this query that means everything matches perfectly]	
SELECT
	* 
	FROM 
	Gold.facts_sales AS fs
	LEFT JOIN gold.dim_customer dc
	ON fs.customer_key = dc.customer_key 
	LEFT JOIN Gold.dim_products AS dp
	ON dp.product_key =fs.product_key --> so we are checkinh whether we can connect fact with together dimension that is customers n products
	WHERE dp.product_key IS NULL;
--> [In ouput there in no results that means there is proper matching in all data sets]
