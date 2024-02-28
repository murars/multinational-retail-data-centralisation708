-- The related values in other columns have been checked and there is no valuable value.
DELETE FROM dim_date_times
WHERE date_uuid IS NULL;

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);