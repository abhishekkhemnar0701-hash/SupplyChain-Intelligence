
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL,
    location VARCHAR(100),
    contact_email VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(100),
    unit_price NUMERIC(12,2),
    reorder_level INTEGER DEFAULT 10
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    warehouse_location VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    total_amount NUMERIC(12,2),
    order_status VARCHAR(50),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    supplier_id INTEGER REFERENCES suppliers(supplier_id),
    delivery_status VARCHAR(50),
    expected_date DATE,
    actual_date DATE
);

CREATE TABLE IF NOT EXISTS processed_orders (
    event_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    product_id INTEGER,
    product_name VARCHAR(150),
    quantity INTEGER,
    unit_price NUMERIC(12,2),
    total_amount NUMERIC(14,2),
    order_status VARCHAR(50),
    event_timestamp TIMESTAMP,
    order_date DATE,
    order_year INTEGER,
    order_month INTEGER
);