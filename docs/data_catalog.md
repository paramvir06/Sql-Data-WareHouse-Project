# Data Dcitionry of Gold Layer

## Overview
Gold Layer is Business-level data representaion, structured to support analytic and busines reporting use cases. it consiist of **dimension**
and **Facts Table** for specific business metrics.

### 1)Gold.dim_customers:
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
