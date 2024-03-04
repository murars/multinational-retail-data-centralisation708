SELECT locality, COUNT(store_code) as "tolal_no_stores" FROM dim_store_details
GROUP BY locality
ORDER BY tolal_no_stores DESC