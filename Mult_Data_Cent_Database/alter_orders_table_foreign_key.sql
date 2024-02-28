ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid  -- This names the foreign key constraint
FOREIGN KEY (date_uuid)      -- This specifies the column in orders_table
REFERENCES dim_date_times(date_uuid); -- This specifies the primary key in dim_date_times

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid  
FOREIGN KEY (user_uuid)      
REFERENCES dim_users(user_uuid);

-- the card_numbers are exist in orders_table but not in dim_card_details table 
SELECT DISTINCT card_number
FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

-- delete the card_numbers are exist in orders_table but not in dim_card_details table
DELETE FROM orders_table
WHERE card_number IN ('3543745641013832', '341935091733787', '3512756643215215', '344132437598598','2720312980409662', '4222069242355461965', '3505784569448924', '584541931351', '3535182016456604', '3556268655280464', '5451311230288361', '4982246481860', '3554954842403828', '4217347542710', '4252720361802860591', '630466795154', '3544855866042397', '2604762576985106', '575421945446', '4672685148732305', '4654492346226715', '4971858637664481', '2314734659486501', '213174667750869', '4814644393449676', '38922600092697');

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number  
FOREIGN KEY (card_number)      
REFERENCES dim_card_details(card_number);

SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

-- delete the store_code is exist in orders_table but not in dim_store_details table
DELETE FROM orders_table
WHERE store_code IN ('WEB-1388012W');

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code  
FOREIGN KEY (store_code)      
REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code  
FOREIGN KEY (product_code)      
REFERENCES dim_products(product_code);


