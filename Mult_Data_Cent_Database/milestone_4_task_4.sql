SELECT 
    CASE 
        WHEN dsd.store_type IS NULL THEN 'Web' 
        ELSE 'Offline'
    END AS location, --sales_channel
    COUNT(otw.product_quantity) AS numbers_of_sales,
    SUM(otw.product_quantity) AS product_quantity_count 
FROM 
    "orders_table_WEB" otw
LEFT JOIN 
    dim_products dp ON otw.product_code = dp.product_code
LEFT JOIN 
    dim_store_details dsd ON otw.store_code = dsd.store_code
GROUP BY 
    location;
	

	

