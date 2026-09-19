-- Check row counts
SELECT
    (SELECT COUNT(*) FROM staging.cleaned_data) AS staging_rows,
    (SELECT COUNT(*) FROM core.customers) AS customers,
    (SELECT COUNT(*) FROM core.products) AS products,
    (SELECT COUNT(*) FROM core.orders) AS orders;


-- Check for NULL values in core.orders
SELECT
    COUNT(*) FILTER (WHERE row_id IS NULL) AS row_id_nulls,
    COUNT(*) FILTER (WHERE order_id IS NULL) AS order_id_nulls,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS customer_id_nulls,
    COUNT(*) FILTER (WHERE product_id IS NULL) AS product_id_nulls,
    COUNT(*) FILTER (WHERE order_date IS NULL) AS order_date_nulls,
    COUNT(*) FILTER (WHERE ship_date IS NULL) AS ship_date_nulls,
    COUNT(*) FILTER (WHERE sales IS NULL) AS sales_nulls,
    COUNT(*) FILTER (WHERE quantity IS NULL) AS quantity_nulls,
    COUNT(*) FILTER (WHERE discount IS NULL) AS discount_nulls,
    COUNT(*) FILTER (WHERE unit_price IS NULL) AS unit_price_nulls
FROM core.orders;


-- Check duplicate row IDs
SELECT row_id, COUNT(*)
FROM core.orders
GROUP BY row_id
HAVING COUNT(*) > 1;


-- Check invalid dates
SELECT COUNT(*) AS invalid_dates
FROM core.orders
WHERE ship_date < order_date;


-- Check invalid quantities
SELECT COUNT(*) AS invalid_quantity
FROM core.orders
WHERE quantity <= 0;


-- Check invalid discounts
SELECT COUNT(*) AS invalid_discount
FROM core.orders
WHERE discount < 0 OR discount > 1;


-- Check Sales formula
SELECT COUNT(*) AS invalid_sales
FROM core.orders
WHERE ABS(
    sales - (unit_price * quantity * (1 - discount))
) > 0.01;


-- Check missing customers
SELECT COUNT(*) AS missing_customers
FROM core.orders o
LEFT JOIN core.customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


-- Check missing products
SELECT COUNT(*) AS missing_products
FROM core.orders o
LEFT JOIN core.products p
    ON o.product_id = p.product_id
WHERE p.product_id IS NULL;