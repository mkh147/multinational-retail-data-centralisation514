/*
Milestone 4, Task 1
*/
SELECT 
	country_code AS country,
	COUNT(country_code) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_no_stores DESC

/*
Milestone 4, Task 2
*/
SELECT 
	locality,
	COUNT(locality) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
	locality
HAVING 
	COUNT(locality) >= 10
ORDER BY
	total_no_stores DESC

/*
Milestone 4, Task 3
*/

SELECT
    ROUND(CAST(SUM(o.product_quantity * p.product_price) as NUMERIC), 2) AS total_sales,
    dt.month
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_date_times dt ON o.date_uuid = dt.date_uuid
GROUP BY
    dt.month
ORDER BY
    total_sales DESC
LIMIT 6;

/*
Milestone 4, Task 4
*/

SELECT
    COUNT(*) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE
        WHEN store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table o
JOIN
    dim_store_details dsd ON o.store_code = dsd.store_code
GROUP BY
    location
ORDER BY
    location DESC;

/*
Milestone 4, Task 5
*/

WITH TotalSales AS (
    SELECT
        s.store_type,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM
        orders_table o
    JOIN
        dim_products p ON o.product_code = p.product_code
    JOIN
        dim_store_details s ON o.store_code = s.store_code
    GROUP BY 
        s.store_type
),
OverallSales AS (
    SELECT
        SUM(total_sales) AS overall_total
    FROM
        TotalSales
)
SELECT
    ts.store_type,
    ROUND(CAST(ts.total_sales AS numeric), 2) AS total_sales,
    ROUND((CAST(ts.total_sales AS numeric) / CAST(os.overall_total AS numeric)) * 100, 2) AS percentage_of_total
FROM
    TotalSales ts,
    OverallSales os
ORDER BY 
    total_sales DESC;

/*
Milestone 4, Task 6
*/

SELECT
    ROUND(CAST(SUM(o.product_quantity * p.product_price) AS NUMERIC), 2) AS total_sales,
    dt.year,
    dt.month
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_date_times dt ON o.date_uuid = dt.date_uuid
GROUP BY 
    dt.year, dt.month
ORDER BY 
    total_sales DESC
LIMIT 10;

/*
Milestone 4, Task 7
*/

SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code 
FROM dim_store_details
GROUP BY country_code 
ORDER BY total_staff_numbers DESC;

/*
Milestone 4, Task 8
*/

SELECT
    ROUND(CAST(SUM(o.product_quantity * p.product_price) AS NUMERIC), 2) AS total_sales,
    s.store_type,
    s.country_code
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
JOIN
    dim_store_details s ON o.store_code = s.store_code
WHERE
    s.country_code = 'DE'
GROUP BY
    s.store_type, s.country_code
ORDER BY
    total_sales;

/*
Milestone 4, Task 9
*/

WITH CTE_1 AS (
	SELECT timestamp, year, month, day
	FROM dim_date_times d
	JOIN orders_table o ON d.date_uuid = o.date_uuid
	ORDER BY year, month, day),

	CTE_2 AS(
	SELECT timestamp, year, LEAD(timestamp) over (ORDER BY timestamp) AS timestamp_next
	FROM CTE_1
	ORDER BY YEAR, timestamp),
	
	CTE_3 AS(
		SELECT timestamp, timestamp_next, year, (timestamp_next - timestamp) AS DELTA
		FROM CTE_2
		ORDER BY YEAR, timestamp)

	SELECT year, AVG(DELTA) AS actual_time_taken
	FROM CTE_3
	GROUP BY year	
	ORDER BY year;
	



WITH CTE_1 AS (
	SELECT timestamp, year, month, day
	FROM dim_date_times d
	JOIN orders_table o ON d.date_uuid = o.date_uuid
	ORDER BY year, month, day),

	CTE_2 AS(
	SELECT timestamp, year, LEAD(timestamp) over (ORDER BY timestamp) AS timestamp_next
	FROM CTE_1
	ORDER BY YEAR, timestamp)

		SELECT timestamp, timestamp_next, year, (timestamp_next - timestamp) AS DELTA
		FROM CTE_2
		ORDER BY YEAR, timestamp

	


