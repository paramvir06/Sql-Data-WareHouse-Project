/* =================================================================================================
            DDL Script : Create view for Gold Layer
 ===================================================================================================
Script Purpose :
         This script create viwe for goold layer in dataware house.
         The Gold layer represenst the final dimeansions and fact table (star schema).

Each view perform transformation and combines data from silver layer
to produce clean and businees ready data  sets.

Usage :
       These view can be queried  direclty for analtic and reporting.
====================================================================================================
*/


---------------------------------------------------------------------
-->DDL Script for view gold.dim_customers:
--------------------------------------------------------------------
IF OBJECT_ID('gold.dim_customer','V') IS NOT NULL
DROP VIEW gold.dim_customer;
GO
CREATE VIEW gold.dim_customer AS 
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS firt_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
	WHEN 
	ci.cst_gndr != 'Unknown' THEN ci.cst_gndr  -- CRM is the Correct n main sorce of data 
    ELSE COALESCE(ca.gen,'Unknown')            -- COALESCE(ca.gen,'Unknown') is used to puut unkown in place where ca.gen value is null.SO when there is no data from crm sql will select data from ERP   
	END  AS gender,
	ca.bdate AS birth_date,
	ci.cst_creatadate AS create_date
    FROM Silver.crm_cust_info AS ci
	LEFT JOIN Silver.erp_cust_az12 AS ca
	ON ci.cst_key =ca.cid
	LEFT JOIN silver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid;
GO

---------------------------------------------------------------------
-->DDL Script for view gold.dim_products:
--------------------------------------------------------------------

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
DROP VIEW  gold.dim_products;
GO
CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_date,pn.prd_key) AS product_key, --> [we will be using it to connect our data models]
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_name AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
    pc.subcat AS sub_category,
	pn.prd_cost AS product_cost,
	pc.maintenance,
	pn.prd_line AS product_line,
	pn.prd_start_date AS start_date
    FROM 
	Silver.crm_prd_info AS pn
	LEFT JOIN  Silver.erp_px_cat_g1v2 AS pc
	ON pn.cat_id =  pc.id
	WHERE prd_end_date IS NULL;
GO
---------------------------------------------------------------------
-->DDL Script for view gold.fact_sales:
--------------------------------------------------------------------
IF OBJECT_ID('gold.facts_sales','V') IS NOT NULL
DROP VIEW gold.facts_sales;
GO
CREATE VIEW gold.facts_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,  
	cu.customer_key,   
	sd.sls_order_dt AS order_date,
	sd.sls_ship_date AS shipping_date,
	sd.sls_due_date AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
	FROM Silver.crm_sales_details AS sd
	LEFT JOIN gold.dim_products  AS pr
	ON sd.sls_prod_key = pr.product_number
	LEFT JOIN Gold.dim_customer AS cu
	ON sd.sls_cust_id = cu.customer_id;
