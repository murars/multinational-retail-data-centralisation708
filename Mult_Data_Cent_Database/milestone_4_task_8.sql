SELECT dsd.country_code, dsd.store_type,
	   ROUND(SUM(dp.product_price * ot.product_quantity)::numeric,2) AS total_sales
FROM dim_store_details dsd
JOIN orders_table ot ON ot.store_code = dsd.store_code
JOIN dim_products dp ON dp.product_code = ot.product_code
WHERE dsd.country_code IN ('DE')
GROUP BY dsd.country_code, dsd.store_type
ORDER BY total_sales

