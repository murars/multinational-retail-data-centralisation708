-- The related values in other columns have been checked and there is no valuable value.
DELETE FROM dim_users
WHERE user_uuid IS NULL;

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);