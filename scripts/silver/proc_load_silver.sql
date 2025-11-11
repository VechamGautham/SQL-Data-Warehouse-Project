/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		


Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
as $$
DECLARE 
	t_start timestamp;
	t_end timestamp;
	elapsed interval;
	rows_ bigint;
BEGIN 
-- SILVER.CRM_CUST_INFO
	t_start := clock_timestamp();
	RAISE NOTICE E'\n================ silver.crm_cust_info ================\n';

	
	raise notice '>> Truncating table: silver.crm_cust_info';
	truncate table silver.crm_cust_info;

	raise notice '>> Inserting data into: silver.crm_cust_info';
	insert into silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date)
	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname, -- removes leading and trailing spaces 
	trim(cst_lastname) as cst_lastname,  -- removes leading and trailing spaces 
	
	(case when upper(trim(cst_material_status)) = 'M' then 'Married'
		  when upper(trim(cst_material_status))= 'S' then 'Single'
		  else 'n/a' end) as cst_marital_status, -- normalize marital values and handle missing values 
		  
	(case when upper(trim(cst_gndr)) = 'F' then 'Female'
		  when upper(trim(cst_gndr)) = 'M' then 'Male' 
		  else 'n/a' end) as cst_gndr, -- normalize gender values and handle missing values 
	cst_create_date 
	from (
		select *,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null 
	)as t -- Delete duplicate records by keeping only the most recent one (based on cst_create_date (primary_key))
	where flag_last = 1 ;
	
    raise notice '>> Insert completed successfully for silver.crm_cust_info';

	get diagnostics rows_ = ROW_COUNT; -- row inserted 
	t_end := clock_timestamp();
	elapsed := t_end - t_start ;

	raise notice 'silver.crm_cust_info: inserted % rows in %', rows_, elapsed ;

-- SILVER.CRM_PRD_INFO

	RAISE NOTICE E'\n================ silver.crm_prd_info ================\n';


	t_start := clock_timestamp();
	raise notice '>> Truncating table: silver.crm_prd_info';
	truncate table silver.crm_prd_info;

	raise notice '>> Inserting data into: silver.crm_prd_info';
	insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	select 
		prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id , -- takes first 5 characters of prd_key, replaces '-' with '_' to form category ID (primary key of erp.px_cat_g1v2)
		substring(prd_key,7,length(prd_key)) as prd_key , -- extracts product key starting from 7th character onward (forign key of silver.crm_sales_details) 
		prd_nm,
		coalesce(prd_cost,0) as prd_cost, -- set prd_cost 0 for null values 
		(case when upper(trim(prd_line)) = 'M' then 'Mountain'
			 when upper(trim(prd_line)) = 'R' then 'Road'
			 when upper(trim(prd_line)) = 'S' then 'other_Sales'
			 when upper(trim(prd_line)) = 'T' then 'Touring'
			 else 'n/a' 
	    end) as prd_line,-- normalize prd_line values and handle missing values 
		cast (prd_start_dt as date) as prd_start_dt, -- convert timestamp data type into date type 
	    cast ((LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt ) - interval '1 day') as date) as prd_end_dt -- sets prd_end_dt as one day before the next prd_start_dt for the same prd_key
	
	from bronze.crm_prd_info ;

	raise notice '>> Insert completed successfully for silver.crm_prd_info';

	get diagnostics rows_ = ROW_COUNT ;
	t_end := clock_timestamp();
	elapsed := t_end - t_start ;

	raise notice 'silver.crm_prd_info: inserted % rows in %', rows_, elapsed ;
	

-- SILVER.CRM_SALES_DETAILS

	RAISE NOTICE E'\n================ silver.crm_sales_details ================\n';

	t_start := clock_timestamp();
	raise notice '>> Truncating table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;

	raise notice '>> Inserting data into: silver.crm_sales_details';
	insert into silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	
	case when sls_order_dt = 0 or length(sls_order_dt::text) != 8 then null 
	else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt, -- convert int into dates and change wrong values into null
	
	case when sls_ship_dt = 0 or length(sls_ship_dt::text) != 8 then null 
	else cast(cast(sls_ship_dt as varchar) as date) end as sls_ship_dt, -- convert int into dates and change wrong values into null
	
	case when sls_due_dt = 0 or length(sls_due_dt::text) != 8 then null 
	else cast(cast(sls_due_dt as varchar) as date) end as sls_due_dt, -- convert int into dates and change wrong values into null
	
	case when (sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)) 
			then sls_quantity * abs(sls_price) 
		else sls_sales 
	end as sls_sales, -- Recalculate slaes if original value in missing or incorrect 
	
	sls_quantity,
	
	case when sls_price is null or sls_price <= 0 
			then sls_sales/ sls_quantity
		else sls_price 
	end as sls_price -- derive price if original value is invalid 
	
	from bronze.crm_sales_details;
	
	raise notice '>> Insert completed successfully for silver.crm_sales_details';

	get diagnostics rows_ = ROW_COUNT ;
	t_end := clock_timestamp();
	elapsed := t_end - t_start ;

	raise notice 'silver.crm_sales_details: inserted % rows in %', rows_, elapsed ;

-- SILVER.ERP_CUST_AZ12

	RAISE NOTICE E'\n================ silver.erp_cust_az12 ================\n';

	t_start := clock_timestamp();
	raise notice '>> Truncating table: silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;

	raise notice '>> Inserting data into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	select 
	
	case when cid like 'NAS%' then substring(cid,4,length(cid)) 
		else cid end as cid, -- remove 'NAS' prefix if present (forign key of silver.crm_cust_info)
		
	case when bdate > now() then null 
		else bdate 
	end as bdate ,-- set future birthdates to null 
	
	case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		 when upper(trim(gen)) in ('M','MALE') then 'Male'
		 else 'n/a'
	end as gen -- normalize gender values and handle unknown cases 
	from bronze.erp_cust_az12 ;
	
    RAISE NOTICE '>> Insert completed successfully for silver.erp_cust_az12';
	
	get diagnostics rows_ = ROW_COUNT ;
	t_end := clock_timestamp() ;
	elapsed := t_end - t_start ;

	raise notice 'silver.erp_cust_az12: inserted % rows in %', rows_, elapsed ;

-- SILVER.ERP_LOC_A101

	RAISE NOTICE E'\n================ silver.erp_loc_a101 ================\n';


	t_start := clock_timestamp() ;
	raise notice '>> Truncating table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;

	raise notice '>> Inserting data into: silver.erp_erp_loc_a101';
	insert into silver.erp_loc_a101 (
		cid,
		cntry
	)
	select 
	replace(cid,'-','') as cid , -- remove - from the cid (forign key of silver.crm_cust_info)
	case when upper(trim(cntry)) = 'DE' then 'Germany'
		 when upper(trim(cntry)) in ('US','USA') then 'United States'
		 when trim(cntry) = '' or cntry is null then 'n/a'
		 else cntry -- normalize and handle missing or blank country codes 
	end as cntry 
	from bronze.erp_loc_a101 ;
	RAISE NOTICE '>> Insert completed successfully for silver.erp_loc_a101';

	get diagnostics rows_ = ROW_COUNT ;
	t_end := clock_timestamp() ;
	elapsed := t_end - t_start ;

	raise notice 'silver.erp_loc_a101: inserted % rows in %', rows_, elapsed ;

-- SILVER.ERP_PX_CAT_G1V2

	RAISE NOTICE E'\n================ silver.erp_px_cat_g1v2 ================\n';

	t_start := clock_timestamp();
	raise notice '>> Truncating table: silver.erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;

	raise notice '>> Inserting data into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	select 
	id,
	cat,
	subcat,
	maintenance 
	from bronze.erp_px_cat_g1v2 ;
	RAISE NOTICE '>> Insert completed successfully for silver.erp_px_cat_g1v2';

	get diagnostics rows_ = ROW_COUNT ;
	t_end := clock_timestamp() ;
	elapsed := t_end - t_start ;

	raise notice 'silver.erp_px_cat_g1v2: inserted % rows in %', rows_, elapsed ;
	
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '!! silver.load_silver failed: %', SQLERRM;
        RAISE;
END;
$$;






