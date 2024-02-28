ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR
USING TO_CHAR(expiry_date,'MM/YY');

ALTER TABLE dim_card_details
 ALTER COLUMN card_number TYPE VARCHAR(25),
 ALTER COLUMN expiry_date TYPE VARCHAR(25),
 ALTER COLUMN date_payment_confirmed TYPE DATE;
	