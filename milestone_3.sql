
--Task 1: Cast columns of orders_table to the correct data types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

--Verify orders_table data types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'orders_table';

-Task 2: Cast columns of dim_users to the correct data types
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(10),
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

--Verify dim_users data types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';


--Task 3: Cast columns of dim_store_details to the correct data types
-- Update the latitude column with values from the lat column if latitude is NULL
UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat::FLOAT);

--Drop the lat column
ALTER TABLE dim_store_details
DROP COLUMN lat;

--Cast new column data types
-ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(20), -- Change 20 to the required length based on your data
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(10), -- Change 10 to the required length based on your data
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Change 'N/A' values to NULL in the `locality` column
UPDATE dim_store_details
    SET locality = NULL
    WHERE locality = 'N/A';

--Task 4: Remove the £ character from the product_price column
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');


-- Populate the weight_class column based on weight ranges
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

--Task 5: Update the dim_products table

-- Step 1: Create a new boolean column `still_available` with default FALSE
ALTER TABLE dim_products
    ADD COLUMN still_available BOOLEAN DEFAULT FALSE;

-- Step 2: Update the `still_available` column based on the `removed` values
UPDATE dim_products
SET still_available = CASE 
    WHEN removed ILIKE 'still_avaliable' THEN TRUE 
    ELSE FALSE 
END;

-- Step 3: Drop the `removed` column
ALTER TABLE dim_products
    DROP COLUMN removed;

--Step 4: Update the table data types
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(255),
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
    ALTER COLUMN weight_class TYPE VARCHAR(255);

-- Task 6: Alter the dim_date_times table to update column data types
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(10),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(20),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

--Task 7: Update the dim_card_details table

--Step1: Find the maximum lengths of  card number and expiry data, and change the columns data types
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(22),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

--Task 8: Assigning foreign keys

-- Add primary key to dim_store_details
ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_code PRIMARY KEY (store_code);

-- Add primary key to dim_products
ALTER TABLE dim_products
ADD CONSTRAINT pk_product_code PRIMARY KEY (product_code);

-- Add primary key to dim_date_times
ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_uuid PRIMARY KEY (date_uuid);

START TRANSACTION 
DELETE FROM dim_card_details
WHERE ctid NOT IN (
    SELECT min(ctid)
    FROM dim_card_details
    GROUP BY card_number
);

-- Step 3: Add primary key constraint to card_number
ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);


-- Add primary key to dim_card_details
ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);

-- Add primary key to dim_users (assuming the table exists)
ALTER TABLE dim_users
ADD CONSTRAINT pk_user_uuid PRIMARY KEY (user_uuid);

INSERT INTO dim_card_details (card_number, expiry_date, date_payment_confirmed)
SELECT DISTINCT card_number, '12/99', NOW()
FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

--Task 9: Create the foreign keys

-- Create foreign key constraint for user_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

-- Create foreign key constraint for card_number
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

-- Create foreign key constraint for date_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

-- Create foreign key constraint for product_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

-- Create foreign key constraint for store_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
