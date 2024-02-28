UPDATE dim_date_times
SET date_uuid = NULL
WHERE date_uuid NOT SIMILAR TO '[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}'
OR date_uuid = 'Default Value';

ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(25),
	ALTER COLUMN year TYPE VARCHAR(25),            
	ALTER COLUMN day TYPE VARCHAR(25),
	ALTER COLUMN time_period TYPE VARCHAR(25),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;