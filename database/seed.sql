
-- Reset sample data for repeatable local testing
TRUNCATE TABLE deliveries, orders, inventory, products, suppliers
RESTART IDENTITY CASCADE;

-- Suppliers
INSERT INTO suppliers
(supplier_name, location, contact_email)
VALUES
('ABC Suppliers', 'Pune', 'abc@example.com'),
('Global Distributors', 'Mumbai', 'global@example.com'),
('Prime Logistics', 'Nashik', 'prime@example.com');

-- Products
INSERT INTO products
(product_name, category, unit_price, reorder_level)
VALUES
('Laptop', 'Electronics', 55000, 10),
('Mobile Phone', 'Electronics', 25000, 15),
('Office Chair', 'Furniture', 7500, 8),
('Desk Table', 'Furniture', 12000, 5),
('Headphones', 'Accessories', 2000, 20);

-- Inventory
INSERT INTO inventory
(product_id, stock_quantity, warehouse_location)
VALUES
(1, 25, 'Pune Warehouse'),
(2, 12, 'Mumbai Warehouse'),
(3, 40, 'Pune Warehouse'),
(4, 7, 'Nashik Warehouse'),
(5, 100, 'Mumbai Warehouse');

-- Orders
INSERT INTO orders
(product_id, quantity, total_amount, order_status)
VALUES
(1, 2, 110000, 'Delivered'),
(2, 3, 75000, 'Processing'),
(3, 5, 37500, 'Delivered'),
(4, 2, 24000, 'Shipped'),
(5, 10, 20000, 'Processing');

-- Deliveries
INSERT INTO deliveries
(order_id, supplier_id, delivery_status, expected_date, actual_date)
VALUES
(1, 1, 'Delivered', '2026-11-15', '2026-11-15'),
(2, 2, 'In Transit', '2026-11-20', NULL),
(3, 3, 'Delivered', '2026-11-14', '2026-11-16'),
(4, 1, 'In Transit', '2026-11-21', NULL),
(5, 2, 'Processing', '2026-11-22', NULL);
