DELETE FROM dim_products
WHERE product_code = 'Default Value' 


ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);