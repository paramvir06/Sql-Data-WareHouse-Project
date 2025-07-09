# Data Dictionary of Gold Layer

## Overview
Gold Layer is Business-level data representaion, structured to support analytic and busines reporting use cases. it consiist of **dimension**
and **Facts Table** for specific business metrics.

### 1) Gold.dim_customers:
**Purpose:** Store customers details enriched with demographic and geographic data.

| Column Name       | Data type    | Description                                                                                                 |
|:-----------------:|:-----------: |:-----------------------------------------------------------------------------------------------------------:|
| customer_key      | int          | Surrogate key uniquely idenity each customer record in dimension table                                      |                                                          
| customer_id       | int          | unique numerical identifier assigned to each customer                                                       |
|customer_number    |NVARCHAR(50)  | alphanumeric identifier representing the customers used for trackng and refrencing                          |
|first_name         |NVARCHAR(50)  | The customers first name as recorded in the sysytem                                                         |
|last_name          |NVARCHAR(50)  | The customers last name as recorded in the sysytem                                                          |
|country            |NVARCHAR(50)  | The country of the residence of the customers                                                               |
|martial_status     |NVARCHAR(50)  | The marital status of the customers (e.g.marries/single)                                                    |
|gender             |NVARCHAR(50)  | The Gender of the customer whether male or female                                                           |
|birth_date         |date          | The date of birth of customer yy-mm-dd formats                                                              |  
|create_date        |date          | Exact time when customer record was created in the system                                                   | 

### 2) Gold.dim_products:
**Purpose:** Store customers details enriched with demographic and geographic data.

| Column Name         | Data type    | Description                                                                                                 |
|:-------------------:|:-----------: |:-----------------------------------------------------------------------------------------------------------:|
|product_key          |int           | Surrogate key uniquely idenity each product record in product dimension table                               |
|product_id           |int           | unique numerical identifier assigned top product for internal tracking                                      | 
|product_number       |NAVCHAR(50)   | A structured alphanumeric code representing the product                                                     |
|category_id          |NAVCHAR(50)   | A unique identifier for product category                                                                    |
|sub_category         |NAVCHAR(50)   | A more detailed classificaton of product witin category                                                     |
|maintenance_required |NAVCHAR(50)   | It indicates whether product requird maintenance or not yes or no                                           |
|cost                 |int           | The cost price of the products                                                                              |
|product_line         |NAVCHAR(50)   | This specifies product line or series to which product belongs(e.g. Road,Mountains)                         |
|start_date           |date          | Thde date when product available for sale or use,stored in                                                  |

### 3) Gold.facts_sales
**Purose :** Store transactional sales data for analytical purpose

| Column Name         | Data type    | Description                                                                                                 |
|:-------------------:|:-----------: |:-----------------------------------------------------------------------------------------------------------:|
|order_number         |NVARCHAR(50)  | A unique alphanumeric identifier for each sales order                                                       |
|product_key          |INT           | Surogate key linking order to product dimension                                                             |  
|customer_key         |INT           | Surrogate key  linking order to customes details dimensions                                                 |
|order_date           |DATE          | A date when order as placed                                                                                 |
|shipping_date        |DATE          | A date when order was shipped to the customers                                                              |    
|due_date             |DATE          | A date when order payment was due                                                                           |
|sales_amount         |INT           | The total monetary value or tge sales                                                                       |
|quantity             |INT           | The number of unit of the products ordered                                                                  |
|amount               |INT           | The price of the unit of the products                                                                       |





























