INSERT INTO customers (customer_name, email, phone_number)
VALUES 
('Emily Mensah', 'emily.mensah@example.com', '0201112233'),
('Frank Owusu', 'frank.owusu@example.com', '0507654321'),
('Grace Bediako', 'grace.bediako@example.com', '0274455667'),
('Henry Nartey', 'henry.nartey@example.com', '0243344556'),
('Ivy Asante', 'ivy.asante@example.com', '0231122334'),
('Alice Johnson', 'alice.johnson@example.com', '0551234567'),
('Brian Smith', 'brian.smith@example.com', '0249876543'),
('Cynthia Adams', 'cynthia.adams@example.com', '0263214321'),
('Daniel Boateng', 'daniel.boateng@example.com', '0207654321');


INSERT INTO products (product_name, category_id, price, stock_quantity, reorder_level)
VALUES
('Laptop Pro 15"', 'Electronics', 1200.00, 15, 5),
('Wireless Mouse', 'Accessories', 25.00, 40, 10),
('Office Chair', 'Furniture', 150.00, 5, 5),
('Standing Desk', 'Furniture', 300.00, 3, 2),
('Smartphone X', 'Electronics', 850.00, 20, 7),
('Noise Cancelling Headphones', 'Electronics', 200.00, 12, 4),
('Webcam 1080p', 'Accessories', 75.00, 25, 10),
('Portable SSD 1TB', 'Electronics', 130.00, 18, 6),
('Monitor 27"', 'Electronics', 300.00, 7, 3),
('Graphic Tablet', 'Accessories', 90.00, 10, 5);


INSERT INTO orders (order_date, total_amount)
VALUES
('2025-04-10', 1250.00),
('2025-04-11', 875.00),
('2025-04-12', 50.00),
('2025-04-13', 450.00),
('2025-04-14', 600.00),   
('2025-04-14', 390.00),   
('2025-04-14', 930.00),   
('2025-04-13', 400.00),   
('2025-04-12', 1250.00);  


-- Order 5: Ivy buys 2 headphones, 2 webcams
INSERT INTO OrderDetails (order_id, product_id, Quantity, Price)
VALUES 
(5, 6, 2, 200.00),
(5, 7, 2, 75.00);

-- Order 6: Henry buys 3 SSDs
INSERT INTO OrderDetails (order_id, product_id, Quantity, Price)
VALUES 
(6, 8, 3, 130.00);

-- Order 7: Emily buys 1 monitor, 1 graphic tablet
INSERT INTO OrderDetails (order_id, product_id, Quantity, Price)
VALUES 
(7, 9, 1, 300.00),
(7, 10, 1, 90.00);

-- Order 8: Frank buys 1 laptop, 2 desks
INSERT INTO OrderDetails (order_id, product_id, Quantity, Price)
VALUES 
(8, 1, 1, 1200.00),
(8, 4, 2, 300.00);

-- Order 9: Grace buys 4 chairs and 1 smartphone
INSERT INTO OrderDetails (order_id, product_id, Quantity, Price)
VALUES 
(9, 3, 4, 150.00),
(9, 5, 1, 850.00);


-- Headphones and webcams (Order 5)
INSERT INTO InventoryLogs (product_id, ChangeType, quantity_changed)
VALUES 
(6, 'Order', -2),
(7, 'Order', -2);

-- SSDs (Order 6)
INSERT INTO InventoryLogs (product_id, ChangeType, quantity_changed)
VALUES 
(8, 'Order', -3);

-- Monitor & Tablet (Order 7)
INSERT INTO InventoryLogs (product_id, ChangeType, quantity_changed)
VALUES 
(9, 'Order', -1),
(10, 'Order', -1);

-- Laptop and desks (Order 8)
INSERT INTO InventoryLogs (product_id, ChangeType, quantity_changed)
VALUES 
(1, 'Order', -1),
(4, 'Order', -2);

-- Chairs & smartphone (Order 9)
INSERT INTO InventoryLogs (product_id, ChangeType, quantity_changed)
VALUES 
(3, 'Order', -4),
(5, 'Order', -1);


SELECT * FROM customers;
SELECT * FROM inventorylogs;
SELECT * FROM orderdetails;
SELECT * FROM orders;
SELECT * FROM products;
