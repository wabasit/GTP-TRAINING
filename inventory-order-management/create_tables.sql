CREATE TABLE products(
	product_id INT PRIMARY KEY IDENTITY (1, 1),
	product_name VARCHAR(30) NOT NULL,
	category_id VARCHAR(30),
	price DECIMAL (10, 2),
	stock_quantity INT,
	reorder_level INT 

);

CREATE TABLE customers(
	customer_id INT PRIMARY KEY IDENTITY (100, 1), 
	customer_name VARCHAR(30) NOT NULL, 
	email VARCHAR(30), 
	phone_number VARCHAR(15)

);

CREATE TABLE orders(
	order_id INT PRIMARY KEY IDENTITY (1000, 1), 
	customer_id INT, 
	order_date DATETIME, 
	total_amount INT,
	FOREIGN KEY(customer_id) REFERENCES customers(customer_id)

);

CREATE TABLE orderdetails (
    order_detail_id INT PRIMARY KEY IDENTITY(1,1),
    order_id INT,                                   
    product_id INT,                                
    quantity INT,                                  
    price DECIMAL(10,2),                           
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);

CREATE TABLE inventorylogs (
    log_id INT PRIMARY KEY IDENTITY(1,1),           
    product_id INT,                                 
    changeDate DATETIME DEFAULT GETDATE(),         
    changeType VARCHAR(50),                        
    quantity_changed INT,                           
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);