DELETE FROM dim_store_details 
WHERE store_code = 'NULL'

ALTER TABLE dim_store_details
ADD PRIMARY KEY(store_code)