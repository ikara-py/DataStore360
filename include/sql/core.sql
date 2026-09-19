CREATE TABLE IF NOT EXISTS core.customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS core.products (
    product_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    sub_category TEXT NOT NULL,
    product_name TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS core.orders (
    row_id INTEGER PRIMARY KEY,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE NOT NULL,
    ship_mode TEXT NOT NULL,
    country TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    postal_code TEXT,
    region TEXT NOT NULL,
    sales NUMERIC NOT NULL,
    quantity INTEGER NOT NULL,
    discount NUMERIC NOT NULL,
    delivery_time INTERVAL NOT NULL,
    unit_price NUMERIC NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES core.customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES core.products(product_id)
);