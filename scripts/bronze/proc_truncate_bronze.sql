/*
===============================================================================
Stored Procedure: Truncate Bronze Tables
===============================================================================
Script Purpose:
    This stored procedure truncates all specified tables in the 'bronze' schema.
    It performs the following actions:
    - Removes all data from the listed bronze tables.
    - Keeps the table structure (schema, columns, constraints, etc.) intact.

Usage Example:
    CALL truncate_bronze_tables();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE truncate_bronze_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE 
        bronze.crm_prd_info,
        bronze.crm_cust_info,
        bronze.crm_sales_info,
    		bronze.erp_cust_az12,
    		bronze.erp_loc_a101,
    		bronze.erp_px_cat_g1v2

    RAISE NOTICE 'Bronze tables truncated successfully.';
END;
$$;
