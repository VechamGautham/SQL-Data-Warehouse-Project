/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- Create Dimension: gold.dim_customers 
create or replace view  gold.dim_customers as 
select 
	row_number() over(order by cust.cst_id) as customer_key, -- Surrogate key 
	cust.cst_id as customer_id,
	cust.cst_key as customer_number,
	cust.cst_firstname as first_name,
	cust.cst_lastname as last_name,
	erp_loc.cntry as country,
	cust.cst_marital_status as marital_status ,
	case when cust.cst_gndr != 'n/a' then cust.cst_gndr -- CRM is the primary source for gender 
		else coalesce(erp_cust.gen,'n/a') -- Fallback to ERP data 
	end as gender, 
	erp_cust.bdate as birthdate,
	cust.cst_create_date as create_date
from silver.crm_cust_info as cust
left join silver.erp_cust_az12 erp_cust
on 			cust.cst_key = erp_cust.cid

left join silver.erp_loc_a101 erp_loc
on 			cust.cst_key = erp_loc.cid ;


-- Create Dimension: gold.dim_products 
create or replace view if not exists gold.dim_products as 
select 
	row_number() over(order by pn.prd_start_dt,pn.prd_key) as product_key, -- Surrogate key 
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category ,
	pc.subcat as subcategory,
	pc.maintenance as maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date 
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc 

on pn.cat_id = pc.id 
where pn.prd_end_dt is null -- Filter out all historical data ;


-- Create Fact Table: gold.fact_sales 
create or replace view if not exists gold.fact_sales as 
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price 
from silver.crm_sales_details sd 
left join gold.dim_products pr 
on sd.sls_prd_key = pr.product_number 

left join gold.dim_customers cu 
on sd.sls_cust_id = cu.customer_id ;


