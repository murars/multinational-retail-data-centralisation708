SELECT 
    ddt.month,
    ddt.year,
    ROUND(SUM(dp.product_price * otw.product_quantity)::numeric, 2) AS total_sales
FROM 
   public."orders_table_WEB" otw
LEFT JOIN 
    public.dim_products dp ON otw.product_code = dp.product_code
LEFT JOIN 
	public.dim_date_times ddt ON otw.date_uuid::uuid = ddt.date_uuid
GROUP BY 
    ddt.month, ddt.year
ORDER BY 
 	ddt.year, CAST(ddt.month AS INTEGER);