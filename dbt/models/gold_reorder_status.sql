WITH product_sales AS (
    SELECT 
        product_id,
        COUNT(order_id) AS total_units_sold,
        SUM(reordered) AS total_times_reordered
    FROM {{ source('fabric_staging', 'stg_order_products') }}
    GROUP BY product_id
),

product_info AS (
    SELECT 
        p.product_id,
        p.product_name,
        d.department
    FROM {{ source('fabric_staging', 'stg_products') }} p
    LEFT JOIN {{ source('fabric_staging', 'stg_departments') }} d 
        ON p.department_id = d.department_id
)

SELECT 
    i.product_id,
    i.product_name,
    i.department,
    COALESCE(s.total_units_sold, 0) AS total_units_sold,
    COALESCE(s.total_times_reordered, 0) AS total_times_reordered,
    CASE 
        WHEN s.total_times_reordered > 5000 THEN 'Critical Restock'
        WHEN s.total_times_reordered > 1000 THEN 'Normal Restock'
        ELSE 'Stock Sufficient'
    END AS inventory_status
FROM product_info i
LEFT JOIN product_sales s 
    ON i.product_id = s.product_id