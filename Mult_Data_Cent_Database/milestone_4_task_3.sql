SELECT 
    dd.month, 
    ROUND(SUM(ot.product_quantity * dp.product_price)::numeric, 2) AS total_sales
FROM
    orders_table ot
INNER JOIN 
    dim_products dp ON ot.product_code = dp.product_code
INNER JOIN 
    dim_date_times dd ON ot.date_uuid = dd.date_uuid
GROUP BY 
    dd.month
ORDER BY 
    total_sales DESC;