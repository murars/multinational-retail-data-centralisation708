SELECT 
    COALESCE(inner_query.store_type, 'Web Portal') AS store_type, 
    inner_query.total_sales,
    ROUND((inner_query.total_sales / overall_total.total_sales_overall * 100), 2) AS percentage_total
FROM 
    (
        SELECT 
            dsd.store_type,
            ROUND(SUM(otw.product_quantity * dp.product_price)::numeric, 2) AS total_sales
        FROM 
            "orders_table_WEB" otw
        LEFT JOIN 
            dim_products dp ON otw.product_code = dp.product_code
        LEFT JOIN 
            dim_store_details dsd ON otw.store_code = dsd.store_code
        GROUP BY 
            dsd.store_type
    ) AS inner_query,
    (
        SELECT 
            ROUND(SUM(otw.product_quantity * dp.product_price)::numeric, 2) AS total_sales_overall
        FROM 
            "orders_table_WEB" otw
        LEFT JOIN 
            dim_products dp ON otw.product_code = dp.product_code
    ) AS overall_total;