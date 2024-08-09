/*
Milestone 3, Task 1
*/
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid :: UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid:: UUID,
ALTER COLUMN card_number TYPE VARCHAR (19),
ALTER COLUMN store_code TYPE VARCHAR (12),
ALTER COLUMN product_code TYPE VARCHAR (11),
ALTER COLUMN product_quantity TYPE SMALLINT;

/*
Milestone 3, Task 2
*/

-- to check what length to set VARCHAR (?)
SELECT MAX(LENGTH(country_code)) FROM dim_users

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR (255),
ALTER COLUMN last_name TYPE VARCHAR (255),	
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR (2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid:: UUID,
ALTER COLUMN join_date TYPE DATE;

/*
Milestone 3, Task 3
*/

-- merge two latitude columns into one
UPDATE dim_store_details
SET latitude = lat
WHERE latitude IS NULL;

-- Drop the latitude2 column
ALTER TABLE dim_store_details
DROP COLUMN lat;

-- to check what length to set VARCHAR (?)
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;
SELECT MAX(LENGTH(store_code)) FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
ALTER COLUMN locality TYPE VARCHAR (255),
ALTER COLUMN store_code TYPE VARCHAR (12),	
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR (255),
ALTER COLUMN country_code TYPE VARCHAR (2),
ALTER COLUMN continent TYPE VARCHAR (255);

/*
Milestone 3, Task 4
*/

-- remove £ character
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

-- add weight_class column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

/*
Milestone 3, Task 5
*/

-- to check what length to set VARCHAR (?)
SELECT MAX(LENGTH(EAN)) FROM dim_products;
SELECT MAX(LENGTH(product_code)) FROM dim_products;
SELECT MAX(LENGTH(weight_class)) FROM dim_products;

--  rename the removed column to still_available 
ALTER TABLE dim_products
RENAME COLUMN removed TO still_avaliable;

-- returning true or false
UPDATE dim_products
SET still_avaliable = CASE
    WHEN still_avaliable = 'Still_avaliable' THEN 'true'
    WHEN still_avaliable = 'removed' THEN 'false'
END;


ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
ALTER COLUMN weight TYPE FLOAT USING product_price::double precision,
ALTER COLUMN product_code TYPE VARCHAR (12),
ALTER COLUMN date_added TYPE DATE USING date_added::date,
ALTER COLUMN uuid TYPE UUID USING uuid:: UUID,
ALTER COLUMN still_avaliable TYPE BOOL USING still_avaliable::boolean, 
ALTER COLUMN weight_class TYPE VARCHAR (14);

/*
Milestone 3, Task 6
*/

-- to check what length to set VARCHAR (?)
SELECT MAX(LENGTH(month)) FROM dim_date_times;
SELECT MAX(LENGTH(year)) FROM dim_date_times;
SELECT MAX(LENGTH(day)) FROM dim_date_times;
SELECT MAX(LENGTH(time_period)) FROM dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR (2),
ALTER COLUMN year TYPE VARCHAR (4),
ALTER COLUMN day TYPE VARCHAR (2),
ALTER COLUMN time_period TYPE VARCHAR (10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid:: UUID;

/*
Milestone 3, Task 7
*/

-- to check what length to set VARCHAR (?)
SELECT MAX(LENGTH(CARD_NUMBER))FROM dim_card_details;
SELECT MAX(LENGTH(EXPIRY_DATE))FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;	

/*
Milestone 3, Task 8
*/

-- Add primary key 
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);


/*
Milestone 3, Task 9
*/

-- adding Foreign Key relationship: orders_table and dim_products 
ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_products FOREIGN KEY (product_code) REFERENCES dim_products (product_code);

-- adding Foreign Key relationship: orders_table and dim_users 
ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_users FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);

-- adding Foreign Key relationship: orders_table and dim_store_details 
ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_store FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);


-- adding Foreign Key relationship: orders_table and dim_date_times 
ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_date FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);


-- adding Foreign Key relationship: orders_table and dim_card_details 
ALTER TABLE orders_table
    ADD CONSTRAINT fk_orders_card FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);









