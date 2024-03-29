ALTER TABLE dim_users
  ALTER COLUMN first_name TYPE VARCHAR(50),
  ALTER COLUMN last_name TYPE VARCHAR(50),
  ALTER COLUMN date_of_birth TYPE TIMESTAMP WITHOUT TIME ZONE,
  ALTER COLUMN company TYPE VARCHAR(50),
  ALTER COLUMN email_address TYPE VARCHAR(50),
  ALTER COLUMN address TYPE VARCHAR(255),
  ALTER COLUMN country TYPE VARCHAR(50),
  ALTER COLUMN country_code TYPE VARCHAR(50),
  ALTER COLUMN phone_number TYPE VARCHAR(25),
  ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::uuid);
 
 
