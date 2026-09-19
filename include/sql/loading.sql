-- Load cleaned staging data into core tables

INSERT INTO core.customers (
    customer_id,
    customer_name,
    segment
)

SELECT DISTINCT
    "Customer ID",
    "Customer Name",
    "Segment"
FROM staging.cleaned_data;


INSERT INTO core.products (
    product_id,
    category,
    sub_category,
    product_name
)
SELECT DISTINCT
    "Product ID",
    "Category",
    "Sub-Category",
    "Product Name"
FROM staging.cleaned_data;


INSERT INTO core.orders (
    row_id,
    order_id,
    customer_id,
    product_id,
    order_date,
    ship_date,
    ship_mode,
    country,
    city,
    state,
    postal_code,
    region,
    sales,
    quantity,
    discount,
    delivery_time,
    unit_price
)
SELECT
    "Row ID",
    "Order ID",
    "Customer ID",
    "Product ID",
    "Order Date",
    "Ship Date",
    "Ship Mode",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region",
    "Sales",
    "Quantity",
    "Discount",
    "Ship Duration",
    "UnitPrice"
FROM staging.cleaned_data;