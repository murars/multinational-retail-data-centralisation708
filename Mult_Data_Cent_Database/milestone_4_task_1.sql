SELECT country_code AS "Country", COUNT(store_code) AS "Total Number of Stores" FROM public.dim_store_details
GROUP BY country_code
ORDER BY "Total Number of Stores" DESC;

