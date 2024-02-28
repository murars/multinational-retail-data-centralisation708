SELECT card_number FROM dim_card_details
WHERE card_number !~ '^[0-9]+$';


-- The raw data and the related values in other columns have been checked and there is no way to infer a reasonable value.
DELETE FROM dim_card_details
WHERE card_number !~ '^[0-9]+$';


ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

