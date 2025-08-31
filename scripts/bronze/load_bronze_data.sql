/*
============================================================================
Loading bronze layer stored procedure
============================================================================

Script Purpose: Loads CRM and ERP data into tables in the bronze schema.  
    - Tables are truncated prior to loading data from source CSV files.
    - Set the path to each CSV file.
    - Docker: Copy the CSV files to the Postgres container prior to execution.

Parameters:
    None

    This stored procedure does not accept any input vales nor does it return any values.
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    total_start TIMESTAMP := clock_timestamp();
    total_end   TIMESTAMP;
    elapsed INTERVAL;
BEGIN
    RAISE NOTICE '===========================================================';
    RAISE NOTICE 'Loading bronze layer';
    RAISE NOTICE '===========================================================';

    RAISE NOTICE '-----------------------------------------------------------';
    RAISE NOTICE 'Loading CRM data';
    RAISE NOTICE '-----------------------------------------------------------';

    -- Table: crm_cust_info
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        FROM '/opt/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_cust_info loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- Table: crm_prd_info
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        FROM '/opt/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_prd_info loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_prd_info: %', SQLERRM;
    END;

    -- Table: crm_sales_details
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM '/opt/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_sales_details loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading crm_sales_details: %', SQLERRM;
    END;

    RAISE NOTICE '-----------------------------------------------------------';
    RAISE NOTICE 'Loading ERP data';
    RAISE NOTICE '-----------------------------------------------------------';

    -- Table: erp_cust_az12
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12 (cid, bdate, gen)
        FROM '/opt/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_cust_az12 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    -- Table: erp_loc_a101
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101 (cid, cntry)
        FROM '/opt/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_loc_a101 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    -- Table: erp_px_cat_g1v2
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        FROM '/opt/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_px_cat_g1v2 loaded in %', end_time - start_time;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;

    -- Total elapsed
    total_end := clock_timestamp();
    elapsed := total_end - total_start;
    RAISE NOTICE 'All bronze tables loaded in %', elapsed;

END;
$$;
