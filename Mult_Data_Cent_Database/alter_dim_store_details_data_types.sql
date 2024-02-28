ALTER TABLE dim_store_details
  ALTER COLUMN longitude TYPE FLOAT USING product_price::FLOAT,
  ALTER COLUMN locality TYPE VARCHAR(255),
  ALTER COLUMN store_code TYPE VARCHAR(50),
  ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
  ALTER COLUMN opening_date TYPE DATE,
  ALTER COLUMN store_type TYPE VARCHAR(255),
  ALTER COLUMN latitude TYPE FLOAT USING product_price::FLOAT,
  ALTER COLUMN country_code TYPE VARCHAR(50),
  ALTER COLUMN continent TYPE VARCHAR(25);
  


  