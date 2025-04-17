USE inventory-order-management

-- Creating products table
CREATE TABLE products(
	product_id INT PRIMARY KEY IDENTITY (1, 1),
	product_name VARCHAR(30) NOT NULL,
	category_id VARCHAR(30),
	price DECIMAL (10, 2),
	stock_quantity INT,
	reorder_level INT 

);

-- Creating customers table
CREATE TABLE customers(
	customer_id INT PRIMARY KEY IDENTITY (100, 1), 
	customer_name VARCHAR(30) NOT NULL, 
	email VARCHAR(30), 
	phone_number VARCHAR(15)

);

-- Creating orders table
CREATE TABLE orders(
	order_id INT PRIMARY KEY IDENTITY (1000, 1), 
	customer_id INT, 
	order_date DATETIME, 
	total_amount INT,
	FOREIGN KEY(customer_id) REFERENCES customers(customer_id)

);

-- Creating orderdetails table
CREATE TABLE orderdetails (
    order_detail_id INT PRIMARY KEY IDENTITY(1,1),
    order_id INT,                                   
    product_id INT,                                
    quantity INT,                                  
    price DECIMAL(10,2),                           
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

-- Creating inventorylogs table
CREATE TABLE inventorylogs (
    log_id INT PRIMARY KEY IDENTITY(1,1),           
    product_id INT,                                 
    changeDate DATETIME DEFAULT GETDATE(),         
    changeType VARCHAR(50),                        
    quantity_changed INT,                           
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Viewing the tables created
SELECT * FROM customers;
SELECT * FROM inventorylogs;
SELECT * FROM orderdetails;
SELECT * FROM orders;
SELECT * FROM products;

-- Populating data into the customers, products, and orders table for query testing

-- Populating data into the customers table
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

-- Populating data into the products table
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

--Implementing the stored procedure to place orders
EXEC PlaceOrder @customer_id = 100, @product_id = 6, @quantity = 3;

-- Summaries of order
SELECT o.order_id, o.order_date, o.total_amount, od.quantity
FROM orders o
JOIN orderdetails od ON o.order_id = od.order_id
WHERE customer_id = 100;

-- Low on stocks
SELECT product_id, product_name
FROM products
WHERE stock_quantity < reorder_level;

--Customer spending habit
SELECT c.customer_id, o.total_amount,
	CASE
		WHEN o.total_amount >= 1000 THEN 'GOLD'
		WHEN o.total_amount >= 500 THEN 'SILVER'
		ELSE 'BRONZE'
	END AS SpendingHabit
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;

--Discount on large orders
SELECT od.quantity, od.price AS actual_price,
	CASE
		WHEN od.quantity >=20 THEN od.price * 0.85
		WHEN od.quantity >=10 THEN od.price * 0.90
		ELSE od.price
	END AS discounted_price
FROM orderdetails od;

-- Customer categorization based on spending habit
SELECT c.customer_id, SUM(o.total_amount) AS TotalSpent,
       CASE
           WHEN SUM(o.total_amount) >= 1000 THEN 'GOLD'
           WHEN SUM(o.total_amount) >= 500 THEN 'SILVER'
           ELSE 'BRONZE'
       END AS SpendingTier
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;

-- Order views for simplifying access
CREATE VIEW vw_OrderSummary AS
SELECT o.order_id, c.customer_name AS CustomerName, o.order_date, o.total_amount,
       COUNT(od.product_id) AS ItemsOrdered
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN orderdetails od ON o.order_id = od.order_id;

-- View for low stocks
CREATE VIEW vw_LowStock AS
SELECT product_id, stock_quantity, reorder_level,
       (reorder_level - stock_quantity) AS Deficit
FROM products
WHERE stock_quantity < ReorderLevel;


