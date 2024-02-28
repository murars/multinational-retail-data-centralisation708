ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET uuid = NULL
WHERE NOT (uuid ~ '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')
OR uuid = 'Default Value';

UPDATE dim_products
SET still_available ='TRUE'
WHERE still_available ='Still_avaliable';

UPDATE dim_products
SET still_available = 'FALSE'
WHERE still_available = 'Removed';

UPDATE dim_products
SET still_available = NULL
WHERE still_available = 'Default Value';


ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
	ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(50),
	ALTER COLUMN product_code TYPE VARCHAR(25),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
	ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
	ALTER COLUMN weight_class TYPE VARCHAR(25); 
	

	