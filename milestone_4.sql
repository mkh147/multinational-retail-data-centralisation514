-- 4.0: Query to get the number of stores per country
SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 4.1: Query to get the number of stores per country
SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 4.2: Query to get the number of stores per locality
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 10; -- Adjust the limit as needed to get the top localities

-- 4.4: Query to calculate number of sales and product quantity for online and offline purchases
SELECT 
    COUNT(*) AS numbers_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
    CASE 
        WHEN d.store_type = 'web portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM orders_table o
JOIN dim_store_details d ON o.store_code = d.store_code
GROUP BY location
ORDER BY location;

-- 4.6: Query to calculate total and percentage of sales by store type
WITH total_sales AS (
    SELECT 
        d.store_type,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM orders_table o
    JOIN dim_store_details d ON o.store_code = d.store_code
    JOIN dim_products p ON o.product_code = p.product_code
    GROUP BY d.store_type
),
total_revenue AS (
    SELECT SUM(total_sales) AS grand_total
    FROM total_sales
)
SELECT 
    t.store_type,
    t.total_sales,
    ROUND(CAST((t.total_sales / tr.grand_total) * 100 AS NUMERIC), 2) AS percentage_total
FROM total_sales t, total_revenue tr
ORDER BY t.total_sales DESC;

-- 4.7: Query to calculate total sales by year and month
SELECT 
    SUM(o.product_quantity * p.product_price) AS total_sales,
    dt.year,
    dt.month
FROM orders_table o
JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY dt.year, dt.month
ORDER BY total_sales DESC
LIMIT 10;

-- 4.8: Step 1: Reconstruct the timestamp with time period mapping
WITH reconstructed_times AS (
    SELECT 
        o.date_uuid,
        TO_TIMESTAMP(
            CONCAT_WS(' ', 
                CONCAT(dt.year, '-', dt.month, '-', dt.day),
                CASE 
                    WHEN dt.time_period = 'Late_Hours' THEN '22:00:00'
                    WHEN dt.time_period = 'Morning' THEN '08:00:00'
                    WHEN dt.time_period = 'Midday' THEN '12:00:00'
                    WHEN dt.time_period = 'Evening' THEN '18:00:00'
                END
            ), 
            'YYYY-MM-DD HH24:MI:SS'
        ) AS full_timestamp,
        dt.year
    FROM orders_table o
    JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
),
-- Step 2: Calculate time differences between consecutive sales
sales_times AS (
    SELECT 
        full_timestamp,
        year,
        LEAD(full_timestamp) OVER (PARTITION BY year ORDER BY full_timestamp) AS next_timestamp
    FROM reconstructed_times
),
time_diffs AS (
    SELECT 
        year,
        next_timestamp - full_timestamp AS time_diff
    FROM sales_times
    WHERE next_timestamp IS NOT NULL
),
-- Step 3: Calculate average time taken between sales for each year
average_time AS (
    SELECT 
        year,
        AVG(EXTRACT(EPOCH FROM time_diff)) AS avg_time_seconds
    FROM time_diffs
    GROUP BY year
)
-- Step 4: Format the results to display hours, minutes, seconds, and milliseconds
SELECT 
    year,
    CONCAT(
        '"hours": ', FLOOR(avg_time_seconds / 3600), ', ',
        '"minutes": ', FLOOR((avg_time_seconds % 3600) / 60), ', ',
        '"seconds": ', FLOOR(avg_time_seconds % 60), ', ',
        '"milliseconds": ', ROUND((avg_time_seconds - FLOOR(avg_time_seconds)) * 1000)
    ) AS actual_time_taken
FROM average_time
ORDER BY year;