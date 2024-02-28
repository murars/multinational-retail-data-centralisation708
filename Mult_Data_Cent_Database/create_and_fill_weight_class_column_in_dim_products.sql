-- Creat 'weight_class' column 
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(25);

-- Remove non-numeric characters from the weight column
UPDATE dim_products
SET weight = REPLACE(weight, 'kg', '');

-- Convert the weight column to a floating point number
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

-- Fill 'weight_class' according to requirements  
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END;


